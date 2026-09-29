/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zaxxer.hikari.util;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.locks.LockSupport.unpark;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a FIFO queue of
 * registered waiters to manage cross-thread signaling; when threads are
 * waiting, returned items are handed to the longest-waiting thread first.
 * <p>
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @author Brett Wooldridge
 *
 * @param <T> the templated type to store in the bag
 * @hidden
 */
@SuppressWarnings("DuplicatedCode")
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);
   private static final MethodHandle THREAD_IS_VIRTUAL = resolveThreadIsVirtual();

   private static final Object CANCELLED = new Object();
   private static final VarHandle HANDOFF;

   static {
      try {
         HANDOFF = MethodHandles.lookup().findVarHandle(Waiter.class, "handoff", Object.class);
      }
      catch (final ReflectiveOperationException e) {
         throw new ExceptionInInitializerError(e);
      }
   }

   private final CopyOnWriteArrayList<T> sharedList;
   private final boolean useWeakThreadLocals;

   private final ThreadLocal<List<Object>> threadLocalList;
   private final IBagStateListener listener;
   private final AtomicInteger waiters;
   private volatile boolean closed;

   private final ConcurrentLinkedQueue<Waiter> handoffQueue;

   /**
    * A registered waiter for a bag entry.  Waiters are enqueued in the
    * {@link #handoffQueue} in arrival order, and entries are always handed
    * to the longest-waiting (head) waiter first.  The {@link #handoff} field
    * is {@code null} while the waiter is pending, {@link #CANCELLED} once the
    * waiter has given up (timeout, interruption, or satisfied by the shared
    * list), or the entry delivered by a returning thread.
    */
   private static final class Waiter
   {
      final Thread thread = Thread.currentThread();
      volatile Object handoff;
   }

   private static MethodHandle resolveThreadIsVirtual()
   {
      try {
         return MethodHandles.publicLookup()
            .findVirtual(Thread.class, "isVirtual", MethodType.methodType(boolean.class));
      }
      catch (NoSuchMethodException | IllegalAccessException e) {
         return null;
      }
   }

   static boolean isCurrentThreadVirtual()
   {
      if (THREAD_IS_VIRTUAL == null) {
         return false;
      }

      try {
         return (boolean) THREAD_IS_VIRTUAL.invokeExact(Thread.currentThread());
      }
      catch (Throwable t) {
         return false;
      }
   }

   /**
    * This interface defines the contract for an entry in the ConcurrentBag.
    * It provides methods to manage the state of the entry, which can be
    * "not in use", "in use", "removed", or "reserved".
    * @hidden
    */
   public interface IConcurrentBagEntry
   {
      int STATE_NOT_IN_USE = 0;
      int STATE_IN_USE = 1;
      int STATE_REMOVED = -1;
      int STATE_RESERVED = -2;

      boolean compareAndSet(int expectState, int newState);
      void setState(int newState);
      int getState();
   }

   /**
    * This interface defines a listener that will be notified when items
    * are added to the bag, allowing for external management of bag state.
    * The listener is typically used to inform the pool manager about the
    * number of items waiting for a bag item to become available.
    * @hidden
    */
   public interface IBagStateListener
   {
      void addBagItem(int waiting);
   }

   /**
    * Construct a ConcurrentBag with the specified listener.
    *
    * @param listener the IBagStateListener to attach to this bag
    */
   public ConcurrentBag(final IBagStateListener listener)
   {
      this.listener = listener;
      this.useWeakThreadLocals = useWeakThreadLocals();

      this.handoffQueue = new ConcurrentLinkedQueue<>();
      this.waiters = new AtomicInteger();
      this.sharedList = new CopyOnWriteArrayList<>();
      this.threadLocalList = ThreadLocal.withInitial(() ->
         useWeakThreadLocals ? new ArrayList<>(16) : new FastList<>(IConcurrentBagEntry.class, 16)
      );
   }

   /**
    * The method will borrow a BagEntry from the bag, blocking for the
    * specified timeout if none are available.
    *
    * @param timeout how long to wait before giving up, in units of unit
    * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
    * @return a borrowed instance from the bag or null if a timeout occurs
    * @throws InterruptedException if interrupted while waiting
    */
   public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
   {
      // Try the thread-local list first
      final var list = threadLocalList.get();
      for (var i = list.size() - 1; i >= 0; i--) {
         final var entry = list.remove(i);
         @SuppressWarnings("unchecked")
         final T bagEntry = useWeakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
         if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
            return bagEntry;
         }
      }

      // Register as a waiter *before* scanning the shared list.  Combined with
      // requite() publishing the entry before consulting the waiter count, this
      // guarantees that a concurrently returned entry is either handed directly
      // to a registered waiter or is visible to our shared-list scan below.
      final var waiter = new Waiter();
      final var waiting = waiters.incrementAndGet();
      handoffQueue.offer(waiter);

      T racedHandoff = null;
      try {
         // Otherwise, scan the shared list
         for (T bagEntry : sharedList) {
            if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               // If we may have stolen another waiter's connection, request another bag add.
               if (waiting > 1) {
                  listener.addBagItem(waiting - 1);
               }
               if (!HANDOFF.compareAndSet(waiter, null, CANCELLED)) {
                  // a returning thread raced us and won; we cannot use both entries
                  racedHandoff = handoffOf(waiter);
               }
               handoffQueue.remove(waiter);
               return bagEntry;
            }
         }

         listener.addBagItem(waiting);

         // The deadline is computed once, from the moment the wait starts, and is not
         // extended by spurious wake-ups.
         var remaining = timeUnit.toNanos(timeout);
         do {
            final var start = currentTime();
            parkNanos(remaining);
            if (Thread.interrupted()) {
               if (!HANDOFF.compareAndSet(waiter, null, CANCELLED)) {
                  racedHandoff = handoffOf(waiter);
               }
               handoffQueue.remove(waiter);
               throw new InterruptedException();
            }

            final var handoff = waiter.handoff;
            if (handoff != null) {
               handoffQueue.remove(waiter);
               @SuppressWarnings("unchecked")
               final T bagEntry = (T) handoff;
               if (bagEntry.getState() == STATE_IN_USE) {
                  return bagEntry;
               }
               // The entry was removed from the bag while in flight to us;
               // re-register and keep waiting on the remaining time.
               waiter.handoff = null;
               handoffQueue.offer(waiter);
            }

            remaining -= elapsedNanos(start);
         } while (remaining > 10_000);

         if (HANDOFF.compareAndSet(waiter, null, CANCELLED)) {
            handoffQueue.remove(waiter);
            return null;
         }

         // a returning thread delivered an entry just as we gave up; it is ours
         handoffQueue.remove(waiter);
         final T bagEntry = handoffOf(waiter);
         if (bagEntry.getState() == STATE_IN_USE) {
            return bagEntry;
         }
         return null;
      }
      finally {
         waiters.decrementAndGet();
         if (racedHandoff != null && racedHandoff.compareAndSet(STATE_IN_USE, STATE_NOT_IN_USE)) {
            // The entry was claimed for us by a returning thread, but we already
            // have one (or can no longer use it); hand it to the next in line.
            tryHandoff(racedHandoff);
         }
      }
   }

   /**
    * This method will return a borrowed object to the bag.  Objects
    * that are borrowed from the bag but never "requited" will result
    * in a memory leak.
    *
    * @param bagEntry the value to return to the bag
    * @throws NullPointerException if value is null
    * @throws IllegalStateException if the bagEntry was not borrowed from the bag
    */
   public void requite(final T bagEntry)
   {
      bagEntry.setState(STATE_NOT_IN_USE);

      if (tryHandoff(bagEntry)) {
         return;
      }

      final var threadLocalEntries = this.threadLocalList.get();
      if (threadLocalEntries.size() < 16) {
         threadLocalEntries.add(useWeakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
      }
   }

   /**
    * Add a new object to the bag for others to borrow.
    *
    * @param bagEntry an object to add to the bag
    */
   public void add(final T bagEntry)
   {
      if (closed) {
         LOGGER.info("ConcurrentBag has been closed, ignoring add()");
         throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
      }

      sharedList.add(bagEntry);

      // hand the entry directly to the longest-waiting borrower, if any
      tryHandoff(bagEntry);
   }

   /**
    * Remove a value from the bag.  This method should only be called
    * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
    *
    * @param bagEntry the value to remove
    * @return true if the entry was removed, false otherwise
    * @throws IllegalStateException if an attempt is made to remove an object
    *         from the bag that was not borrowed or reserved first
    */
   public boolean remove(final T bagEntry)
   {
      if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
         return false;
      }

      final var removed = sharedList.remove(bagEntry);
      if (!removed && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
      }

      threadLocalList.get().remove(bagEntry);

      return removed;
   }

   /**
    * Close the bag to further adds.
    */
   @Override
   public void close()
   {
      closed = true;
   }

   /**
    * This method provides a "snapshot" in time of the BagEntry
    * items in the bag in the specified state.  It does not "lock"
    * or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in list before performing any action on them.
    *
    * @param state one of the {@link IConcurrentBagEntry} states
    * @return a possibly empty list of objects having the state specified
    */
   public List<T> values(final int state)
   {
      final var list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
      Collections.reverse(list);
      return list;
   }

   /**
    * This method provides a "snapshot" in time of the bag items.  It
    * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in the list, or understand the concurrency implications of
    * modifying items, before performing any action on them.
    *
    * @return a possibly empty list of (all) bag items
    */
   @SuppressWarnings("unchecked")
   public List<T> values()
   {
      return (List<T>) sharedList.clone();
   }

   /**
    * The method is used to make an item in the bag "unavailable" for
    * borrowing.  It is primarily used when wanting to operate on items
    * returned by the <code>values(int)</code> method.  Items that are
    * reserved can be removed from the bag via <code>remove(T)</code>
    * without the need to unreserve them.  Items that are not removed
    * from the bag can be make available for borrowing again by calling
    * the <code>unreserve(T)</code> method.
    *
    * @param bagEntry the item to reserve
    * @return true if the item was able to be reserved, false otherwise
    */
   public boolean reserve(final T bagEntry)
   {
      return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
   }

   /**
    * This method is used to make an item reserved via <code>reserve(T)</code>
    * available again for borrowing.
    *
    * @param bagEntry the item to unreserve
    */
   @SuppressWarnings("SpellCheckingInspection")
   public void unreserve(final T bagEntry)
   {
      if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
         // hand the entry directly to the longest-waiting borrower, if any
         tryHandoff(bagEntry);
      }
      else {
         LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
      }
   }

   /**
    * Get the number of threads pending (waiting) for an item from the
    * bag to become available.
    *
    * @return the number of threads waiting for items from the bag
    */
   public int getWaitingThreadCount()
   {
      return waiters.get();
   }

   /**
    * Attempt to hand an entry directly to a waiting borrower.  Waiters are
    * served strictly first-come, first-served: the entry is always offered to
    * the head of the waiter queue, and is claimed (STATE_NOT_IN_USE to
    * STATE_IN_USE) on behalf of that waiter before it is delivered, so that
    * no later borrower can steal it from the shared list while it is in
    * flight.
    *
    * @param bagEntry the entry to hand off, in STATE_NOT_IN_USE
    * @return true if the entry was delivered to a waiter (or was concurrently
    *         taken by a shared-list scan), false if no thread is waiting
    */
   private boolean tryHandoff(final T bagEntry)
   {
      final var isVirtualThread = isCurrentThreadVirtual();
      for (int i = 1, waiting = waiters.get(); waiting > 0; i++, waiting = waiters.get()) {
         final var waiter = handoffQueue.peek();
         if (waiter == null) {
            // a borrower has registered its intent but has not linked its node
            // into the queue yet; back off briefly and re-check
            backoff(isVirtualThread, i, waiting);
            continue;
         }

         if (!bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
            // taken by a concurrent shared-list scan; no longer ours to give
            return true;
         }

         if (HANDOFF.compareAndSet(waiter, null, bagEntry)) {
            handoffQueue.remove(waiter);
            unpark(waiter.thread);
            return true;
         }

         // the head waiter gave up concurrently; unclaim the entry, prune the
         // cancelled waiter, and try the next one in line
         if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_NOT_IN_USE)) {
            // removed or reserved while in flight; no longer ours to give
            return true;
         }
         handoffQueue.remove(waiter);
      }

      return false;
   }

   private static void backoff(final boolean isVirtualThread, final int i, final int waiting)
   {
      if (isVirtualThread || (i & 0xff) == 0xff || (waiting > 1 && i % waiting == 0)) {
         parkNanos(MICROSECONDS.toNanos(10));
      }
      else {
         Thread.yield();
      }
   }

   @SuppressWarnings("unchecked")
   private T handoffOf(final Waiter waiter)
   {
      return (T) waiter.handoff;
   }

   /**
    * Get a count of the number of items in the specified state at the time of this call.
    *
    * @param state the state of the items to count
    * @return a count of how many items in the bag are in the specified state
    */
   public int getCount(final int state)
   {
      var count = 0;
      for (var e : sharedList) {
         if (e.getState() == state) {
            count++;
         }
      }
      return count;
   }

   public int[] getStateCounts()
   {
      final var states = new int[6];
      for (var e : sharedList) {
         ++states[e.getState()];
      }
      states[4] = sharedList.size();
      states[5] = waiters.get();

      return states;
   }

   /**
    * Get the total number of items in the bag.
    *
    * @return the number of items in the bag
    */
   public int size()
   {
      return sharedList.size();
   }

   public void dumpState()
   {
      sharedList.forEach(entry -> LOGGER.info(entry.toString()));
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    *
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals()
   {
      try {
         if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
         }

         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
      }
      catch (SecurityException se) {
         return true;
      }
   }
}
