/**
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.cube.index.pinot.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 * 
 * Utilities to deal with memory mapped buffers, which may not be portable.
 *
 */
public class MmapUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MmapUtils.class);
    private static final AtomicLong DIRECT_BYTE_BUFFER_USAGE = new AtomicLong(0L);
    private static final AtomicLong MMAP_BUFFER_USAGE = new AtomicLong(0L);
    private static final AtomicInteger MMAP_BUFFER_COUNT = new AtomicInteger(0);
    private static final long BYTES_IN_MEGABYTE = 1;//1024L * 1024L;
    private static final Map<ByteBuffer, AllocationContext> BUFFER_TO_CONTEXT_MAP = Collections.synchronizedMap(new IdentityHashMap<ByteBuffer, AllocationContext>());

    public enum AllocationType {
        MMAP, DIRECT_BYTE_BUFFER
    }

    public static class AllocationContext {
        private final String context;
        private final AllocationType allocationType;

        public AllocationContext(String context, AllocationType allocationType) {
            this.context = context;
            this.allocationType = allocationType;
        }

        public String getContext() {
            return context;
        }

        public AllocationType getAllocationType() {
            return allocationType;
        }

        @Override
        public String toString() {
            return context;
        }
    }

    /**
     * Unloads a byte buffer from memory
     *
     * @param buffer The buffer to unload, can be null
     */
    public static void unloadByteBuffer(Buffer buffer) {
        if (null == buffer)
            return;

        // Non direct byte buffers do not require any special handling, since they're on heap
        if (!buffer.isDirect())
            return;

        // Remove usage from direct byte buffer usage
        final int bufferSize = buffer.capacity();
        final AllocationContext bufferContext = BUFFER_TO_CONTEXT_MAP.get(buffer);

        // A DirectByteBuffer can be cleaned up by doing buffer.cleaner().clean(), but this is not a public API. This is
        // probably not portable between JVMs.
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            Method cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean");

            cleanerMethod.setAccessible(true);
            cleanMethod.setAccessible(true);

            // buffer.cleaner().clean()
            Object cleaner = cleanerMethod.invoke(buffer);
            if (cleaner != null) {
                cleanMethod.invoke(cleaner);

                if (bufferContext != null) {
                    switch (bufferContext.allocationType) {
                    case DIRECT_BYTE_BUFFER:
                        DIRECT_BYTE_BUFFER_USAGE.addAndGet(-bufferSize);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Releasing byte buffer of size {} with context {}, allocation after operation {}", bufferSize, bufferContext, getTrackedAllocationStatus());
                        }
                        break;
                    case MMAP:
                        MMAP_BUFFER_USAGE.addAndGet(-bufferSize);
                        MMAP_BUFFER_COUNT.decrementAndGet();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Unmapping byte buffer of size {} with context {}, allocation after operation {}", bufferSize, bufferContext, getTrackedAllocationStatus());
                        }
                        break;
                    default:
                        break;
                    }
                    BUFFER_TO_CONTEXT_MAP.remove(buffer);
                } else {
                    LOGGER.warn("Attempted to release byte buffer of size {} with no context, no deallocation performed.", bufferSize);
                    if (LOGGER.isDebugEnabled()) {
                        List<String> matchingAllocationContexts = new ArrayList<>();

                        synchronized (BUFFER_TO_CONTEXT_MAP) {
                            clearSynchronizedMapEntrySetCache();

                            for (Map.Entry<ByteBuffer, AllocationContext> byteBufferAllocationContextEntry : BUFFER_TO_CONTEXT_MAP.entrySet()) {
                                if (byteBufferAllocationContextEntry.getKey().capacity() == bufferSize) {
                                    matchingAllocationContexts.add(byteBufferAllocationContextEntry.getValue().toString());
                                }
                            }

                            // Clear the entry set cache afterwards so that we don't hang on to stale entries
                            clearSynchronizedMapEntrySetCache();
                        }

                        LOGGER.debug("Contexts with a size of {}: {}", bufferSize, matchingAllocationContexts);
                        LOGGER.debug("Called by: {}", Utils.getCallingMethodDetails());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Caught (ignored) exception while unloading byte buffer", e);
        }
    }

    /**
     * Allocates a direct byte buffer, tracking usage information.
     *
     * @param capacity The capacity to allocate.
     * @param allocationContext The file that this byte buffer refers to
     * @param details Further details about the allocation
     * @return A new allocated byte buffer with the requested capacity
     */
    public static ByteBuffer allocateDirectByteBuffer(int capacity, File allocationContext, String details) {
        String context;
        if (allocationContext != null) {
            context = allocationContext.getAbsolutePath() + " (" + details + ")";
        } else {
            context = "no file (" + details + ")";
        }

        DIRECT_BYTE_BUFFER_USAGE.addAndGet(capacity);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Allocating byte buffer of size {} with context {}, allocation after operation {}", capacity, context, getTrackedAllocationStatus());
        }

        ByteBuffer byteBuffer = null;

        try {
            byteBuffer = ByteBuffer.allocateDirect(capacity);
        } catch (OutOfMemoryError e) {
            LOGGER.error("Ran out of direct memory while trying to allocate {} bytes (context {})", capacity, context, e);
            LOGGER.error("Allocation status {}", getTrackedAllocationStatus());
            Utils.rethrowException(e);
        }

        BUFFER_TO_CONTEXT_MAP.put(byteBuffer, new AllocationContext(context, AllocationType.DIRECT_BYTE_BUFFER));

        return byteBuffer;
    }

    /**
     * Memory maps a file, tracking usage information.
     *
     * @param randomAccessFile The random access file to mmap
     * @param mode The mmap mode
     * @param position The byte position to mmap
     * @param size The number of bytes to mmap
     * @param allocationContext The file that is mmap'ed
     * @param details Additional details about the allocation
     */
    public static MappedByteBuffer mmapFile(RandomAccessFile randomAccessFile, FileChannel.MapMode mode, long position, long size, File allocationContext, String details) throws IOException {
        String context;
        if (allocationContext != null) {
            context = allocationContext.getAbsolutePath() + " (" + details + ")";
        } else {
            context = "no file (" + details + ")";
        }

        MMAP_BUFFER_USAGE.addAndGet(size);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Memory mapping file, mmap size {} with context {}, allocation after operation {}", size, context, getTrackedAllocationStatus());
        }

        MappedByteBuffer byteBuffer = null;

        try {
            byteBuffer = randomAccessFile.getChannel().map(mode, position, size);
            MMAP_BUFFER_COUNT.incrementAndGet();
        } catch (Exception e) {
            LOGGER.error("Failed to mmap file (size {}, context {})", size, context, e);
            LOGGER.error("Allocation status {}", getTrackedAllocationStatus());
            Utils.rethrowException(e);
        }

        BUFFER_TO_CONTEXT_MAP.put(byteBuffer, new AllocationContext(context, AllocationType.MMAP));

        return byteBuffer;
    }

    private static String getTrackedAllocationStatus() {
        long directByteBufferUsage = DIRECT_BYTE_BUFFER_USAGE.get();
        long mmapBufferUsage = MMAP_BUFFER_USAGE.get();
        long mmapBufferCount = MMAP_BUFFER_COUNT.get();

        return "direct " + (directByteBufferUsage / BYTES_IN_MEGABYTE) + " MB, mmap " + (mmapBufferUsage / BYTES_IN_MEGABYTE) + " MB (" + mmapBufferCount + " files), total " + ((directByteBufferUsage + mmapBufferUsage) / BYTES_IN_MEGABYTE) + " MB";
    }

    /**
     * Returns the number of bytes of direct buffers allocated.
     */
    public static long getDirectByteBufferUsage() {
        return DIRECT_BYTE_BUFFER_USAGE.get();
    }

    /**
     * Returns the number of bytes of memory mapped files.
     */
    public static long getMmapBufferUsage() {
        return MMAP_BUFFER_USAGE.get();
    }

    /**
     * Returns the number of memory mapped files.
     */
    public static long getMmapBufferCount() {
        return MMAP_BUFFER_COUNT.get();
    }

    private static void clearSynchronizedMapEntrySetCache() {
        // For some bizarre reason, Collections.synchronizedMap's implementation (at least on JDK 1.8.0.25) caches the
        // entry set, and will thus return stale (and incorrect) values if queried multiple times, as well as cause those
        // entries to not be garbage-collectable. This clears its cache.
        try {
            Class<?> clazz = BUFFER_TO_CONTEXT_MAP.getClass();
            Field field = clazz.getDeclaredField("entrySet");
            field.setAccessible(true);
            field.set(BUFFER_TO_CONTEXT_MAP, null);
        } catch (Exception e) {
            // Well, that didn't work.
        }
    }

    /**
     * Obtains the list of all allocations and their associated sizes. Only meant for debugging purposes.
     */
    public static List<Pair<AllocationContext, Integer>> getAllocationsAndSizes() {
        List<Pair<AllocationContext, Integer>> returnValue = new ArrayList<Pair<AllocationContext, Integer>>();

        synchronized (BUFFER_TO_CONTEXT_MAP) {
            clearSynchronizedMapEntrySetCache();

            Set<Map.Entry<ByteBuffer, AllocationContext>> entries = BUFFER_TO_CONTEXT_MAP.entrySet();

            // Clear the entry set cache afterwards so that we don't hang on to stale entries
            clearSynchronizedMapEntrySetCache();

            for (Map.Entry<ByteBuffer, AllocationContext> bufferAndContext : entries) {
                returnValue.add(new ImmutablePair<AllocationContext, Integer>(bufferAndContext.getValue(), bufferAndContext.getKey().capacity()));
            }
        }

        return returnValue;
    }
}
