/*
 * Copyright (C) 2026 Brett Wooldridge
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

import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;

/**
 * Concurrency, ordering and timeout tests for the {@link ConcurrentBag}
 * wait/handoff path.
 */
public class ConcurrentBagHandoffTest
{
   private static final ConcurrentBag.IBagStateListener NOOP_LISTENER = waiting -> { };

   static final class StubEntry implements IConcurrentBagEntry
   {
      private final AtomicInteger state = new AtomicInteger(STATE_NOT_IN_USE);

      @Override
      public boolean compareAndSet(int expectState, int newState)
      {
         return state.compareAndSet(expectState, newState);
      }

      @Override
      public void setState(int newState)
      {
         state.set(newState);
      }

      @Override
      public int getState()
      {
         return state.get();
      }
   }

   /**
    * With waiters queued on the bag, a requited entry must be delivered to
    * waiters in the order they registered (first-come, first-served).
    */
   @Test
   public void testFifoHandoffOrder() throws Exception
   {
      final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
      final var entry = new StubEntry();
      bag.add(entry);

      // the main thread holds the only entry, so every borrower must wait
      final var borrowed = bag.borrow(1, SECONDS);
      assertNotNull(borrowed);

      final var threadCount = 8;
      final var acquisitionOrder = new ConcurrentLinkedQueue<Integer>();
      final var failures = new ConcurrentLinkedQueue<Throwable>();
      final var threads = new Thread[threadCount];
      for (var i = 0; i < threadCount; i++) {
         final var sequence = i;
         threads[i] = new Thread(() -> {
            try {
               final var bagEntry = bag.borrow(30, SECONDS);
               assertNotNull("waiter " + sequence + " timed out", bagEntry);
               acquisitionOrder.add(sequence);
               bag.requite(bagEntry);
            }
            catch (Throwable t) {
               failures.add(t);
            }
         });
         threads[i].start();
         // stagger registration so the wait order is deterministic
         awaitWaiterCount(bag, i + 1);
         MILLISECONDS.sleep(25);
      }

      bag.requite(borrowed);

      for (final var thread : threads) {
         thread.join(SECONDS.toMillis(30));
      }

      assertTrue("failures in waiter threads: " + failures, failures.isEmpty());
      assertEquals(Integer.valueOf(threadCount), Integer.valueOf(acquisitionOrder.size()));
      var expected = 0;
      for (final var sequence : acquisitionOrder) {
         assertEquals("handoff order must match registration order", Integer.valueOf(expected++), sequence);
      }
      assertEquals(0, bag.getWaitingThreadCount());
   }

   /**
    * A requite while a waiter is registered must hand the entry to that
    * waiter, not park it in the returning thread's local list.
    */
   @Test
   public void testRequiteHandsOffToWaitingThread() throws Exception
   {
      final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
      final var entry = new StubEntry();
      bag.add(entry);

      final var borrowed = bag.borrow(1, SECONDS);
      assertNotNull(borrowed);

      final var acquired = new CountDownLatch(1);
      final var waiter = new Thread(() -> {
         try {
            final var bagEntry = bag.borrow(30, SECONDS);
            if (bagEntry != null) {
               acquired.countDown();
               bag.requite(bagEntry);
            }
         }
         catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      });
      waiter.start();
      awaitWaiterCount(bag, 1);

      bag.requite(borrowed);

      assertTrue("registered waiter did not receive the requited entry", acquired.await(30, SECONDS));
      waiter.join(SECONDS.toMillis(30));
      assertEquals(0, bag.getWaitingThreadCount());
   }

   /**
    * An entry requited with no waiters lands in the returning thread's local
    * list, but must still be borrowable by other threads after the returning
    * thread is gone.
    */
   @Test
   public void testEntryParkedInDeadThreadLocalIsStillBorrowable() throws Exception
   {
      final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
      final var entry = new StubEntry();
      bag.add(entry);

      final var borrower = new Thread(() -> {
         try {
            bag.requite(bag.borrow(1, SECONDS));
         }
         catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      });
      borrower.start();
      borrower.join(SECONDS.toMillis(30));

      // the entry now sits in the terminated thread's local list only
      final var borrowed = bag.borrow(1, SECONDS);
      assertNotNull("entry stranded in a dead thread's local list", borrowed);
      bag.requite(borrowed);
   }

   /**
    * A borrow that times out must return null after roughly the requested
    * timeout (a single deadline, not re-armed per wakeup) and must release
    * its waiter count.
    */
   @Test
   public void testBorrowTimeoutReturnsNullAndReleasesWaiterCount() throws Exception
   {
      final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
      final var entry = new StubEntry();
      bag.add(entry);

      var borrowed = bag.borrow(1, SECONDS);
      assertNotNull(borrowed);

      final var result = new StubEntry[1];
      final var elapsed = new long[1];
      final var waiter = new Thread(() -> {
         final var start = System.nanoTime();
         try {
            result[0] = bag.borrow(300, MILLISECONDS);
         }
         catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
         elapsed[0] = (System.nanoTime() - start) / 1_000_000;
      });
      waiter.start();
      awaitWaiterCount(bag, 1);

      // wake the waiter up a few times without delivering an entry;
      // the deadline must not be re-armed by these spurious wakeups
      for (var i = 0; i < 3; i++) {
         MILLISECONDS.sleep(50);
         LockSupport.unpark(waiter);
      }

      waiter.join(SECONDS.toMillis(30));

      assertNull("borrow should have timed out", result[0]);
      assertTrue("timeout fired too early: " + elapsed[0] + "ms", elapsed[0] >= 250);
      assertTrue("timeout fired too late: " + elapsed[0] + "ms", elapsed[0] < 3_000);
      assertEquals(0, bag.getWaitingThreadCount());
   }

   /**
    * An interrupted wait must propagate InterruptedException and release the
    * waiter count.
    */
   @Test
   public void testInterruptedBorrowReleasesWaiterCount() throws Exception
   {
      final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
      final var entry = new StubEntry();
      bag.add(entry);

      final var borrowed = bag.borrow(1, SECONDS);
      assertNotNull(borrowed);

      final var interrupted = new CountDownLatch(1);
      final var waiter = new Thread(() -> {
         try {
            bag.borrow(30, SECONDS);
         }
         catch (InterruptedException e) {
            interrupted.countDown();
         }
      });
      waiter.start();
      awaitWaiterCount(bag, 1);

      waiter.interrupt();

      assertTrue("waiter was not interrupted", interrupted.await(30, SECONDS));
      waiter.join(SECONDS.toMillis(30));
      assertEquals(0, bag.getWaitingThreadCount());

      bag.requite(borrowed);
   }

   /**
    * Every waiter exit path (success, timeout, interrupt) must decrement the
    * waiter count exactly once, even under churn.
    */
   @Test
   public void testWaiterCountReturnsToZeroUnderChurn() throws Exception
   {
      final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
      bag.add(new StubEntry());

      // the main thread holds the only entry, so every borrower must wait
      final var borrowed = bag.borrow(1, SECONDS);
      assertNotNull(borrowed);

      final var waiterCount = 16;
      final var threads = new Thread[waiterCount];
      for (var i = 0; i < waiterCount; i++) {
         final var useTimeout = (i & 1) == 0;
         threads[i] = new Thread(() -> {
            try {
               if (useTimeout) {
                  bag.borrow(5, SECONDS); // times out
               }
               else {
                  bag.borrow(30, SECONDS); // interrupted below
               }
            }
            catch (InterruptedException e) {
               // expected for half of the waiters
            }
         });
         threads[i].start();
      }

      awaitWaiterCount(bag, waiterCount);

      for (var i = 1; i < waiterCount; i += 2) {
         threads[i].interrupt();
      }

      for (final var thread : threads) {
         thread.join(SECONDS.toMillis(30));
      }

      assertEquals(0, bag.getWaitingThreadCount());
      bag.requite(borrowed);
   }

   /**
    * A requite that lands exactly on a waiter's timeout must not livelock:
    * the departing waiter passes the entry on (or leaves it in the shared
    * list) and exits promptly, even when it is the last waiter.
    */
   @Test
   public void testTimeoutRacingRequiteDoesNotHang() throws Exception
   {
      for (var iteration = 0; iteration < 200; iteration++) {
         final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
         bag.add(new StubEntry());

         final var borrowed = bag.borrow(1, SECONDS);
         assertNotNull(borrowed);

         final var waiter = new Thread(() -> {
            try {
               // short timeout so the requite below races the waiter's exit
               bag.borrow(20, MILLISECONDS);
            }
            catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         });
         waiter.start();

         // wait until the waiter registers (or has already come and gone)
         final var deadline = System.nanoTime() + SECONDS.toNanos(10);
         while (bag.getWaitingThreadCount() != 1 && waiter.isAlive() && System.nanoTime() < deadline) {
            MILLISECONDS.sleep(1);
         }

         // vary the requite timing so it lands before, on, and after the timeout
         MILLISECONDS.sleep(iteration % 30);

         bag.requite(borrowed);

         waiter.join(SECONDS.toMillis(10));
         assertTrue("waiter did not exit after timeout racing requite (iteration " + iteration + ")", !waiter.isAlive());
         assertEquals(0, bag.getWaitingThreadCount());
      }
   }

   /**
    * Stress the borrow/requite handoff: with the pool saturated, no borrower
    * may starve to a timeout while entries are being returned, and entries
    * must never be double-borrowed.
    */
   @Test
   public void testConcurrentBorrowRequiteStress() throws Exception
   {
      final var bag = new ConcurrentBag<StubEntry>(NOOP_LISTENER);
      final var entryCount = 8;
      final var entries = new StubEntry[entryCount];
      for (var i = 0; i < entryCount; i++) {
         entries[i] = new StubEntry();
         bag.add(entries[i]);
      }

      final var threadCount = 16;
      final var iterations = 500;
      final var failures = new CopyOnWriteArrayList<Throwable>();
      final var inUse = new AtomicInteger();
      final var maxInUse = new AtomicInteger();
      final var threads = new Thread[threadCount];
      for (var i = 0; i < threadCount; i++) {
         threads[i] = new Thread(() -> {
            try {
               for (var j = 0; j < iterations; j++) {
                  final var bagEntry = bag.borrow(30, SECONDS);
                  if (bagEntry == null) {
                     failures.add(new AssertionError("borrow timed out while entries were circulating"));
                     return;
                  }
                  final var nowInUse = inUse.incrementAndGet();
                  maxInUse.accumulateAndGet(nowInUse, Math::max);
                  if (nowInUse > entryCount) {
                     failures.add(new AssertionError("more entries in use than exist: " + nowInUse));
                  }
                  if (bagEntry.getState() != STATE_IN_USE) {
                     failures.add(new AssertionError("borrowed entry not in IN_USE state"));
                  }
                  inUse.decrementAndGet();
                  bag.requite(bagEntry);
               }
            }
            catch (Throwable t) {
               failures.add(t);
            }
         });
         threads[i].start();
      }

      for (final var thread : threads) {
         thread.join(SECONDS.toMillis(120));
         assertTrue("stress thread did not finish", !thread.isAlive());
      }

      assertTrue("failures during stress: " + failures, failures.isEmpty());
      assertEquals(0, bag.getWaitingThreadCount());
      assertEquals(0, inUse.get());
      for (final var entry : entries) {
         assertEquals(STATE_NOT_IN_USE, entry.getState());
      }
   }

   private static void awaitWaiterCount(ConcurrentBag<StubEntry> bag, int expected) throws InterruptedException
   {
      final var deadline = System.nanoTime() + SECONDS.toNanos(30);
      while (bag.getWaitingThreadCount() != expected && System.nanoTime() < deadline) {
         MILLISECONDS.sleep(5);
      }
      assertEquals("waiter count did not reach " + expected, expected, bag.getWaitingThreadCount());
   }
}
