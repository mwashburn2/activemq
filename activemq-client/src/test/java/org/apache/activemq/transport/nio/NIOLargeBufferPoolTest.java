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
package org.apache.activemq.transport.nio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class NIOLargeBufferPoolTest {

    @Test
    public void testDisabledByDefault() {
        NIOLargeBufferPool pool = new NIOLargeBufferPool(0, 20);
        assertFalse(pool.isEnabled());
        assertNull(pool.acquire(1024));
    }

    @Test
    public void testBufferSizeIsTotalOverCount() {
        NIOLargeBufferPool pool = new NIOLargeBufferPool(20L * 1024 * 1024, 20);
        assertTrue(pool.isEnabled());
        assertEquals(1024 * 1024, pool.getBufferSize());
        assertEquals(0, pool.getAllocated());
    }

    @Test
    public void testBufferSizeCappedAtMaxArraySize() {
        NIOLargeBufferPool pool = new NIOLargeBufferPool(100L * Integer.MAX_VALUE, 2);
        assertEquals(Integer.MAX_VALUE - 8, pool.getBufferSize());
    }

    @Test
    public void testAcquireIsDirectAndLimitedToFrameSize() {
        NIOLargeBufferPool pool = new NIOLargeBufferPool(2 * 4096, 2);
        ByteBuffer buffer = pool.acquire(3000);
        assertNotNull(buffer);
        assertTrue(buffer.isDirect());
        assertEquals(0, buffer.position());
        assertEquals(3000, buffer.limit());
        assertEquals(4096, buffer.capacity());
    }

    @Test
    public void testOversizeFrameFallsBack() {
        NIOLargeBufferPool pool = new NIOLargeBufferPool(2 * 4096, 2);
        assertNull(pool.acquire(4097));
        assertEquals(1, pool.getOversize());
        assertEquals(0, pool.getAllocated());
    }

    @Test
    public void testExhaustedPoolFallsBackAndReuses() {
        NIOLargeBufferPool pool = new NIOLargeBufferPool(2 * 4096, 2);
        ByteBuffer a = pool.acquire(4096);
        ByteBuffer b = pool.acquire(4096);
        assertNotNull(a);
        assertNotNull(b);
        assertNull(pool.acquire(4096));
        assertEquals(1, pool.getMisses());
        assertEquals(2, pool.getAllocated());

        pool.release(a);
        assertEquals(1, pool.getAvailable());
        ByteBuffer c = pool.acquire(100);
        assertSame(a, c);
        assertEquals(100, c.limit());
        assertEquals(2, pool.getAllocated());
    }

    @Test
    public void testDiscardFreesSlot() {
        NIOLargeBufferPool pool = new NIOLargeBufferPool(4096, 1);
        ByteBuffer a = pool.acquire(4096);
        assertNull(pool.acquire(4096));
        pool.discard(a);
        assertEquals(0, pool.getAllocated());
        ByteBuffer b = pool.acquire(4096);
        assertNotNull(b);
        assertFalse(a == b);
    }
}
