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

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Concurrency, ordering and timeout tests for {@link ConcurrentBag}.
 */
public class ConcurrentBagFairnessTest
{
   private static final int ENTRY_COUNT = 4;

   /**
    * When several threads are waiting, returned entries must be handed to
    * waiters in first-come-first-served registration order.
    */
   @Test
   public void handoffIsFirstComeFirstServed() throws Exception
   {
      final var waiterCount = 8;
      try (var bag = new ConcurrentBag<StubEntry>(waiting -> {})) {
         // borrow every entry so that subsequent borrowers must wait
         final var entries = new ArrayList<StubEntry>();
         for (var i = 0; i < waiterCount; i++) {
            bag.add(new StubEntry());
            entries.add(bag.borrow(1, MILLISECONDS));
            assertNotNull(entries.get(i));
         }

         final var acquisitionOrder = new ConcurrentLinkedQueue<Integer>();
         final var threads = new ArrayList<Thread>();
         for (var i = 0; i < waiterCount; i++) {
            final var index = i;
            final var thread = new Thread(() -> {
               try {
                  final var entry = bag.borrow(30, SECONDS);
                  assertNotNull("waiter " + index + " timed out", entry);
                  acquisitionOrder.add(index);
               }
               catch (final InterruptedException e) {
                  throw new RuntimeException(e);
               }
            });
            thread.start();
            threads.add(thread);
            // wait until the thread has registered as a waiter before starting
            // the next one, so that registration order matches thread index order
            awaitWaiterCount(bag, i + 1);
            MILLISECONDS.sleep(25);
         }

         // return the entries one at a time; each must go to the head waiter
         for (var i = 0; i < waiterCount; i++) {
            bag.requite(entries.get(i));
            while (acquisitionOrder.size() < i + 1) {
               Thread.yield();
            }
         }

         for (final var thread : threads) {
            thread.join(SECONDS.toMillis(30));
         }

         final var expected = IntStream.range(0, waiterCount).boxed().collect(Collectors.toList());
         assertEquals(expected, new ArrayList<>(acquisitionOrder));
         assertEquals(0, bag.getWaitingThreadCount());
      }
   }

   /**
    * An entry returned at the same moment a borrower registers must never be
    * stranded in a thread-local list while the borrower waits.  Hammer the
    * borrow/requite path; any lost hand-off shows up as a borrow timeout.
    */
   @Test
   public void returnedEntriesAreNeverStranded() throws Exception
   {
      final var threads = 8;
      final var iterations = 20_000;
      try (var bag = new ConcurrentBag<StubEntry>(waiting -> {})) {
         for (var i = 0; i < ENTRY_COUNT; i++) {
            bag.add(new StubEntry());
         }

         final var failure = new AtomicReference<Throwable>();
         final var workers = new ArrayList<Thread>();
         for (var t = 0; t < threads; t++) {
            final var thread = new Thread(() -> {
               try {
                  for (var i = 0; i < iterations && failure.get() == null; i++) {
                     final var entry = bag.borrow(10, SECONDS);
                     if (entry == null) {
                        failure.compareAndSet(null, new AssertionError("borrow timed out; an entry was stranded"));
                        return;
                     }
                     bag.requite(entry);
                  }
               }
               catch (final Throwable e) {
                  failure.compareAndSet(null, e);
               }
            });
            thread.start();
            workers.add(thread);
         }

         for (final var worker : workers) {
            worker.join();
         }

         assertNull(failure.get());
         assertEquals(0, bag.getWaitingThreadCount());
         assertEquals(ENTRY_COUNT, bag.getCount(STATE_NOT_IN_USE));
      }
   }

   /**
    * Borrowers that frequently time out must still leave the waiter count and
    * every entry accounted for: no leaked waiters, no vanished entries.
    */
   @Test
   public void waiterCountAndEntriesSurviveTimeoutChurn() throws Exception
   {
      final var threads = 6;
      try (var bag = new ConcurrentBag<StubEntry>(waiting -> {})) {
         for (var i = 0; i < ENTRY_COUNT; i++) {
            bag.add(new StubEntry());
         }

         final var failure = new AtomicReference<Throwable>();
         final var deadline = System.nanoTime() + SECONDS.toNanos(3);
         final var workers = new ArrayList<Thread>();
         for (var t = 0; t < threads; t++) {
            final var thread = new Thread(() -> {
               try {
                  while (System.nanoTime() < deadline && failure.get() == null) {
                     final var entry = bag.borrow(1 + ThreadLocalRandom.current().nextInt(4), MILLISECONDS);
                     if (entry != null) {
                        Thread.yield();
                        bag.requite(entry);
                     }
                  }
               }
               catch (final Throwable e) {
                  failure.compareAndSet(null, e);
               }
            });
            thread.start();
            workers.add(thread);
         }

         for (final var worker : workers) {
            worker.join();
         }

         assertNull(failure.get());
         assertEquals(0, bag.getWaitingThreadCount());
         assertEquals(ENTRY_COUNT, bag.getCount(STATE_NOT_IN_USE));
      }
   }

   /**
    * A borrow that times out must return null close to its deadline and must
    * release its waiter count.
    */
   @Test
   public void timeoutReturnsNullAndReleasesWaiterCount() throws Exception
   {
      try (var bag = new ConcurrentBag<StubEntry>(waiting -> {})) {
         final var start = System.nanoTime();
         assertNull(bag.borrow(250, MILLISECONDS));
         final var elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - start);

         assertTrue("returned too early: " + elapsedMs + "ms", elapsedMs >= 240);
         assertTrue("returned too late: " + elapsedMs + "ms", elapsedMs < SECONDS.toMillis(5));
         assertEquals(0, bag.getWaitingThreadCount());
      }
   }

   /**
    * Spurious wake-ups must not extend the wait: the deadline is fixed when
    * the wait starts, not re-counted after every wake-up.
    */
   @Test
   public void spuriousWakeupsDoNotExtendTimeout() throws Exception
   {
      try (var bag = new ConcurrentBag<StubEntry>(waiting -> {})) {
         final var result = new AtomicReference<String>();
         final var waiter = new Thread(() -> {
            final var start = System.nanoTime();
            try {
               if (bag.borrow(500, MILLISECONDS) == null) {
                  result.set("elapsed=" + NANOSECONDS.toMillis(System.nanoTime() - start));
               }
               else {
                  result.set("unexpectedly borrowed an entry");
               }
            }
            catch (final InterruptedException e) {
               result.set("interrupted");
            }
         });
         waiter.start();
         awaitWaiterCount(bag, 1);

         // keep waking the waiter up without delivering anything; if wake-ups
         // wrongly reset the deadline the waiter would never time out
         final var unparkDeadline = System.nanoTime() + SECONDS.toNanos(2);
         while (waiter.isAlive() && System.nanoTime() < unparkDeadline) {
            java.util.concurrent.locks.LockSupport.unpark(waiter);
            MILLISECONDS.sleep(10);
         }
         waiter.join(SECONDS.toMillis(5));
         assertTrue("waiter did not time out; wake-ups reset the deadline", !waiter.isAlive());

         final var outcome = result.get();
         assertNotNull(outcome);
         assertTrue(outcome, outcome.startsWith("elapsed="));
         final var elapsedMs = Long.parseLong(outcome.substring("elapsed=".length()));
         assertTrue("returned too early: " + elapsedMs + "ms", elapsedMs >= 480);
         assertTrue("deadline was extended by wake-ups: " + elapsedMs + "ms", elapsedMs < 1_000);
         assertEquals(0, bag.getWaitingThreadCount());
      }
   }

   /**
    * An interrupted waiter must see InterruptedException, must release its
    * waiter count, and must not swallow an entry that was concurrently
    * handed to it.
    */
   @Test
   public void interruptedWaiterReleasesCountAndDoesNotLeakEntry() throws Exception
   {
      try (var bag = new ConcurrentBag<StubEntry>(waiting -> {})) {
         final var interrupted = new AtomicReference<Throwable>();
         final var waiter = new Thread(() -> {
            try {
               bag.borrow(30, SECONDS);
            }
            catch (final InterruptedException e) {
               interrupted.set(e);
            }
         });
         waiter.start();
         awaitWaiterCount(bag, 1);
         MILLISECONDS.sleep(50);

         waiter.interrupt();
         waiter.join(SECONDS.toMillis(30));

         assertNotNull("waiter was not interrupted", interrupted.get());
         assertEquals(0, bag.getWaitingThreadCount());

         // an entry added now must still be borrowable
         final var entry = new StubEntry();
         bag.add(entry);
         assertTrue(bag.borrow(1, SECONDS) == entry);
         assertEquals(0, bag.getWaitingThreadCount());
      }
   }

   /**
    * The waiter count visible to the pool must track the actual number of
    * parked borrowers, and every exit path (hand-off, timeout) must
    * decrement it.
    */
   @Test
   public void waiterCountTracksParkedBorrowers() throws Exception
   {
      final var waiterCount = 5;
      try (var bag = new ConcurrentBag<StubEntry>(waiting -> {})) {
         final var acquired = new AtomicInteger();
         final var threads = new ArrayList<Thread>();
         for (var i = 0; i < waiterCount; i++) {
            final var thread = new Thread(() -> {
               try {
                  if (bag.borrow(30, SECONDS) != null) {
                     acquired.incrementAndGet();
                  }
               }
               catch (final InterruptedException e) {
                  throw new RuntimeException(e);
               }
            });
            thread.start();
            threads.add(thread);
         }

         awaitWaiterCount(bag, waiterCount);
         assertEquals(waiterCount, bag.getWaitingThreadCount());

         // satisfy two waiters, let the remaining three time out on their own
         final var satisfied = 2;
         for (var i = 0; i < satisfied; i++) {
            bag.add(new StubEntry());
            while (acquired.get() < i + 1) {
               Thread.yield();
            }
         }
         assertEquals(waiterCount - satisfied, bag.getWaitingThreadCount());

         for (final var thread : threads) {
            thread.interrupt();
            thread.join(SECONDS.toMillis(30));
         }
         assertEquals(0, bag.getWaitingThreadCount());
      }
   }

   private static void awaitWaiterCount(final ConcurrentBag<StubEntry> bag, final int count) throws InterruptedException
   {
      final var deadline = System.nanoTime() + SECONDS.toNanos(30);
      while (bag.getWaitingThreadCount() < count) {
         if (System.nanoTime() > deadline) {
            fail("timed out waiting for " + count + " registered waiters, saw " + bag.getWaitingThreadCount());
         }
         MILLISECONDS.sleep(1);
      }
   }

   static final class StubEntry implements IConcurrentBagEntry
   {
      private final AtomicInteger state = new AtomicInteger(STATE_NOT_IN_USE);

      @Override
      public boolean compareAndSet(final int expectState, final int newState)
      {
         return state.compareAndSet(expectState, newState);
      }

      @Override
      public void setState(final int newState)
      {
         state.set(newState);
      }

      @Override
      public int getState()
      {
         return state.get();
      }
   }
}
