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

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A fixed set of direct buffers shared by all NIO transports for reading frames
 * larger than the transport I/O buffer. Buffers are allocated lazily and reused.
 * When no buffer is free, or the frame is larger than a pooled buffer,
 * {@link #acquire(int)} returns null and the caller reads the frame into a heap buffer.
 *
 * Disabled by default. Enable with the system properties
 * {@value #TOTAL_BYTES_PROPERTY} (total direct memory to dedicate) and
 * {@value #COUNT_PROPERTY} (number of buffers, default 20). Each buffer is
 * totalBytes / count bytes, capped at Integer.MAX_VALUE - 8. MaxDirectMemorySize
 * must leave room for the whole pool.
 */
public final class NIOLargeBufferPool {

    public static final String TOTAL_BYTES_PROPERTY = "org.apache.activemq.transport.nio.largeBufferPool.totalBytes";
    public static final String COUNT_PROPERTY = "org.apache.activemq.transport.nio.largeBufferPool.count";

    private static final Logger LOG = LoggerFactory.getLogger(NIOLargeBufferPool.class);
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    private static final NIOLargeBufferPool INSTANCE = new NIOLargeBufferPool(
        Long.getLong(TOTAL_BYTES_PROPERTY, 0L), Integer.getInteger(COUNT_PROPERTY, 20));

    private final int count;
    private final int bufferSize;
    private final ArrayBlockingQueue<ByteBuffer> free;
    private final AtomicInteger allocated = new AtomicInteger();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong oversize = new AtomicLong();

    public static NIOLargeBufferPool getInstance() {
        return INSTANCE;
    }

    public NIOLargeBufferPool(long totalBytes, int count) {
        if (totalBytes <= 0 || count <= 0 || totalBytes / count <= 0) {
            this.count = 0;
            this.bufferSize = 0;
            this.free = null;
        } else {
            this.count = count;
            this.bufferSize = (int) Math.min(totalBytes / count, MAX_BUFFER_SIZE);
            this.free = new ArrayBlockingQueue<>(count);
            LOG.info("NIO large buffer pool enabled: {} buffers of {} bytes", count, bufferSize);
        }
    }

    public boolean isEnabled() {
        return bufferSize > 0;
    }

    /**
     * @return a direct buffer with position 0 and limit {@code size}, or null if the
     *         caller should allocate on heap
     */
    public ByteBuffer acquire(int size) {
        if (!isEnabled()) {
            return null;
        }
        if (size > bufferSize) {
            oversize.incrementAndGet();
            return null;
        }
        ByteBuffer buffer = free.poll();
        if (buffer == null) {
            buffer = allocate();
            if (buffer == null) {
                misses.incrementAndGet();
                return null;
            }
        }
        hits.incrementAndGet();
        buffer.clear();
        buffer.limit(size);
        return buffer;
    }

    /**
     * Returns a buffer to the pool. Only call when no other thread can still use it.
     */
    public void release(ByteBuffer buffer) {
        buffer.clear();
        if (!free.offer(buffer)) {
            allocated.decrementAndGet();
        }
    }

    /**
     * Gives up a buffer that may still be referenced elsewhere. The slot is freed so a
     * new buffer can be allocated; the old one is left to the garbage collector.
     */
    public void discard(ByteBuffer buffer) {
        allocated.decrementAndGet();
    }

    private ByteBuffer allocate() {
        while (true) {
            int n = allocated.get();
            if (n >= count) {
                return null;
            }
            if (allocated.compareAndSet(n, n + 1)) {
                try {
                    return ByteBuffer.allocateDirect(bufferSize);
                } catch (OutOfMemoryError e) {
                    allocated.decrementAndGet();
                    LOG.warn("NIO large buffer pool could not allocate a {} byte direct buffer: {}", bufferSize, e.getMessage());
                    return null;
                }
            }
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getCount() {
        return count;
    }

    public int getAllocated() {
        return allocated.get();
    }

    public int getAvailable() {
        return free == null ? 0 : free.size();
    }

    public long getHits() {
        return hits.get();
    }

    public long getMisses() {
        return misses.get();
    }

    public long getOversize() {
        return oversize.get();
    }
}
