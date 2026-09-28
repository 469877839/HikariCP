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

package com.zaxxer.hikari.pool;

import static com.zaxxer.hikari.pool.TestElf.getPool;
import static com.zaxxer.hikari.pool.TestElf.newHikariConfig;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.ConcurrentBag;
import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;

/**
 * Verify that when threads are queued waiting for a connection, returned
 * connections are handed to the longest-waiting thread (FIFO), and that
 * newly arriving threads cannot bypass the queue through the thread-local
 * list or the shared list scan.
 */
public class FairHandoffTest
{
   /**
    * With a saturated pool, threads that arrive (and block) in a known order
    * must acquire the single pooled connection in exactly that order as it is
    * returned again and again.
    */
   @Test
   public void testWaitersAreServedInArrivalOrder() throws Exception
   {
      final HikariConfig config = newHikariConfig();
      config.setMinimumIdle(1);
      config.setMaximumPoolSize(1);
      config.setConnectionTimeout(10_000);
      config.setConnectionTestQuery("VALUES 1");
      config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

      try (HikariDataSource ds = new HikariDataSource(config)) {
         final HikariPool pool = getPool(ds);
         final int waiterCount = 8;

         // Hold the only connection so that every other thread must queue up.
         final Connection heldConnection = ds.getConnection();

         final List<Integer> acquisitionOrder = Collections.synchronizedList(new ArrayList<>());
         final List<Thread> waiters = new ArrayList<>();
         for (int i = 0; i < waiterCount; i++) {
            final int index = i;
            final Thread waiter = new Thread(() -> {
               try (Connection connection = ds.getConnection()) {
                  acquisitionOrder.add(index);
               }
               catch (Exception e) {
                  throw new RuntimeException(e);
               }
            });
            waiter.start();
            waiters.add(waiter);

            // Wait until this thread has registered as a waiter, then give it
            // time to actually park on the handoff queue, so that the queue
            // order matches the start order.
            awaitCondition(() -> pool.getThreadsAwaitingConnection() == index + 1);
            Thread.sleep(100);
         }

         // Release the connection; it must ripple through the waiters in FIFO order.
         heldConnection.close();

         for (Thread waiter : waiters) {
            waiter.join(SECONDS.toMillis(15));
         }

         final List<Integer> expected = new ArrayList<>();
         for (int i = 0; i < waiterCount; i++) {
            expected.add(i);
         }
         assertEquals("Connections were not handed off in arrival (FIFO) order", expected, acquisitionOrder);

         // Once the pool is quiet, the accounting invariant must hold.
         awaitCondition(() -> pool.getThreadsAwaitingConnection() == 0);
         assertEquals(0, pool.getActiveConnections());
         assertEquals(pool.getTotalConnections(), pool.getActiveConnections() + pool.getIdleConnections());
      }
   }

   /**
    * White-box test: while a thread is parked waiting, a connection in the
    * middle of being returned (state already NOT_IN_USE, handoff offer not
    * yet attempted) must still reach the waiting thread -- a newly arriving
    * borrower must not snatch it from the shared list.
    */
   @Test
   public void testNewBorrowerCannotBypassWaitingThread() throws Exception
   {
      final ExecutorService executor = Executors.newCachedThreadPool();
      try (ConcurrentBag<GatedEntry> bag = new ConcurrentBag<>(waiting -> { })) {
         final GatedEntry entry = new GatedEntry();
         entry.setState(IConcurrentBagEntry.STATE_IN_USE);
         bag.add(entry);

         // W1 registers and parks, waiting for an entry.
         final Future<GatedEntry> waitingThread = executor.submit(() -> bag.borrow(10, SECONDS));
         awaitCondition(() -> bag.getWaitingThreadCount() == 1);
         Thread.sleep(200); // ensure W1 is parked inside poll()

         // Start returning the entry, but hold the requite inside setState(),
         // after the state became NOT_IN_USE and before the handoff offer.
         entry.armGate();
         final Thread releaser = new Thread(() -> bag.requite(entry));
         releaser.start();
         assertTrue("requite() did not reach setState()", entry.awaitGateArmed());

         // A newcomer now tries to borrow; the entry is idle in the shared
         // list, but W1 is queued ahead of it.
         final Future<GatedEntry> newcomer = executor.submit(() -> bag.borrow(300, MILLISECONDS));
         Thread.sleep(200); // give the newcomer time to try (and fail) to take the entry
         entry.openGate();

         assertSame("The longest-waiting thread did not receive the returned entry", entry, waitingThread.get(5, SECONDS));
         assertNull("A newcomer bypassed the waiting thread and took the entry", newcomer.get(5, SECONDS));

         releaser.join(SECONDS.toMillis(5));
         assertEquals(0, bag.getWaitingThreadCount());
      }
      finally {
         executor.shutdownNow();
      }
   }

   private static void awaitCondition(Condition condition) throws InterruptedException
   {
      final long deadline = System.nanoTime() + SECONDS.toNanos(10);
      while (!condition.isMet()) {
         if (System.nanoTime() > deadline) {
            throw new AssertionError("Timed out waiting for condition");
         }
         Thread.sleep(10);
      }
   }

   private interface Condition
   {
      boolean isMet();
   }

   /**
    * A bag entry whose {@link #setState(int)} can be paused by a test-controlled
    * gate, simulating the window between an entry becoming NOT_IN_USE and the
    * handoff offer that follows it in {@link ConcurrentBag#requite}.
    */
   private static final class GatedEntry implements IConcurrentBagEntry
   {
      private final AtomicInteger state = new AtomicInteger();
      private volatile CountDownLatch gate;
      private volatile CountDownLatch gateArmed;

      void armGate()
      {
         this.gate = new CountDownLatch(1);
         this.gateArmed = new CountDownLatch(1);
      }

      boolean awaitGateArmed() throws InterruptedException
      {
         return gateArmed.await(5, SECONDS);
      }

      void openGate()
      {
         gate.countDown();
      }

      @Override
      public boolean compareAndSet(int expectState, int newState)
      {
         return state.compareAndSet(expectState, newState);
      }

      @Override
      public void setState(int newState)
      {
         state.set(newState);
         final CountDownLatch latch = gate;
         if (latch != null && newState == STATE_NOT_IN_USE) {
            gateArmed.countDown();
            try {
               latch.await();
            }
            catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
      }

      @Override
      public int getState()
      {
         return state.get();
      }
   }
}
