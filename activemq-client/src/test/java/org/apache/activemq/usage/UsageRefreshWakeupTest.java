/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UsageRefreshWakeupTest {

    private ThreadPoolExecutor executor;

    static class ExternalUsage extends Usage<ExternalUsage> {
        final AtomicLong value = new AtomicLong();

        ExternalUsage(String name) {
            super(null, name, 1.0f);
        }

        @Override
        protected long retrieveUsage() {
            return value.get();
        }
    }

    @Before
    public void setUp() {
        executor = new ThreadPoolExecutor(1, 4, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), r -> {
            Thread t = new Thread(r, "UsageRefreshWakeupTest");
            t.setDaemon(true);
            return t;
        });
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    private ExternalUsage fullUsage() {
        ExternalUsage usage = new ExternalUsage("external");
        usage.setExecutor(executor);
        usage.setPollingTime(60000);
        usage.setLimit(100);
        usage.start();
        usage.value.set(150);
        assertEquals(150, usage.getPercentUsage());
        assertTrue(usage.isFull());
        return usage;
    }

    @Test(timeout = 10000)
    public void testRefreshReleasesNotFullCallback() throws Exception {
        ExternalUsage usage = fullUsage();
        final CountDownLatch called = new CountDownLatch(1);
        assertTrue(usage.notifyCallbackWhenNotFull(called::countDown));

        usage.value.set(10);
        assertEquals(10, usage.getPercentUsage());

        assertTrue("callback released by refresh", called.await(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 10000)
    public void testRefreshReleasesWaiter() throws Exception {
        final ExternalUsage usage = fullUsage();
        final CountDownLatch waiting = new CountDownLatch(1);
        final CountDownLatch released = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                waiting.countDown();
                if (usage.waitForSpace(0)) {
                    released.countDown();
                }
            } catch (InterruptedException ignored) {
            }
        }, "waiter");
        waiter.setDaemon(true);
        waiter.start();
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        assertFalse(released.await(500, TimeUnit.MILLISECONDS));

        usage.value.set(10);
        assertEquals(10, usage.getPercentUsage());

        assertTrue("waiter released by refresh", released.await(5, TimeUnit.SECONDS));
        waiter.interrupt();
    }

    @Test(timeout = 10000)
    public void testLimitIncreaseReleasesMemoryUsageWaiter() throws Exception {
        final MemoryUsage usage = new MemoryUsage("limit");
        usage.setExecutor(executor);
        usage.setLimit(100);
        usage.start();
        usage.increaseUsage(150);
        assertTrue(usage.isFull());

        final CountDownLatch called = new CountDownLatch(1);
        assertTrue(usage.notifyCallbackWhenNotFull(called::countDown));
        final CountDownLatch released = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                usage.waitForSpace();
                released.countDown();
            } catch (InterruptedException ignored) {
            }
        }, "memory-waiter");
        waiter.setDaemon(true);
        waiter.start();
        assertFalse(released.await(300, TimeUnit.MILLISECONDS));

        usage.setLimit(1000);

        assertTrue(released.await(5, TimeUnit.SECONDS));
        assertTrue(called.await(5, TimeUnit.SECONDS));
        waiter.interrupt();
        usage.stop();
    }

    @Test(timeout = 20000)
    public void testConcurrentIncreaseDecreaseNoLostWakeups() throws Exception {
        final MemoryUsage usage = new MemoryUsage("stress");
        usage.setExecutor(executor);
        usage.setLimit(1000);
        usage.start();

        final int producers = 8;
        final int iterations = 20000;
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                futures.add(pool.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        usage.increaseUsage(100);
                        usage.getPercentUsage();
                        usage.isFull(100);
                        usage.decreaseUsage(100);
                    }
                    return null;
                }));
            }
            final CountDownLatch done = new CountDownLatch(producers);
            for (int p = 0; p < producers; p++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 200; i++) {
                            usage.waitForSpace();
                        }
                    } catch (InterruptedException ignored) {
                    }
                    done.countDown();
                });
            }
            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
            assertEquals(0, usage.getUsage());
            assertTrue("no waiter stranded", done.await(5, TimeUnit.SECONDS));
            assertFalse(usage.isFull());
            final CountDownLatch last = new CountDownLatch(1);
            if (!usage.notifyCallbackWhenNotFull(last::countDown)) {
                last.countDown();
            }
            assertTrue(last.await(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
            usage.stop();
        }
    }
}