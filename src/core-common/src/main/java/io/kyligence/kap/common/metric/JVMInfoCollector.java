/*
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

package io.kyligence.kap.common.metric;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.sun.management.UnixOperatingSystemMXBean;

public class JVMInfoCollector {
    private static volatile boolean isStarted = false;

    public static final Logger logger = LoggerFactory.getLogger(JVMInfoCollector.class);
    private static final String DB = "KE_METRIC";
    private static final Set youngGCAlgorithm = Sets.newHashSet("Copy", "ParNew", "PS Scavenge", "G1 Young Generation");
    private static final Set oldGCAlgorithm = Sets.newHashSet("MarkSweepCompact", "PS MarkSweep", "ConcurrentMarkSweep", "G1 Old Generation");
    private static final String MEASURE_GC = "gc";
    private static final String MEASURE_OPEN_FD = "openFD";
    private static final String MEASURE_MEM = "memory";

    private JVMInfoCollector() {}

    public static void init(final String identifier) {
        Preconditions.checkNotNull(identifier);
        if (!isStarted) {
            synchronized (JVMInfoCollector.class) {
                if (!isStarted) {
                    isStarted = true;
                    collect(identifier);
                }
            }
        }
    }

    private static void collect(final String identifier) {
        logger.info("Start collect JVM Info");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    MetricWriterStrategy writer = MetricWriterStrategy.INSTANCE;
                    String host = InetAddress.getLocalHost().getHostName();
                    collectGCInfo(host, identifier, writer);
                    collectOpenFD(host, identifier, writer);
                    collectMem(host, identifier, writer);
                } catch (Throwable th) {
                    logger.error("Error when getting JVM info.", th);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private static void collectGCInfo(String host, String identifier, MetricWriterStrategy writer) throws Throwable {
        final List<GarbageCollectorMXBean> gc = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean GarbageCollectorMXBean : gc) {
            String gcAlg = GarbageCollectorMXBean.getName();
            Map<String, Object> fields = new HashMap<>();
            if (youngGCAlgorithm.contains(gcAlg)) {
                fields.put("YoungGCCount", GarbageCollectorMXBean.getCollectionCount());
                fields.put("YoungGCTime", GarbageCollectorMXBean.getCollectionTime());

            } else if (oldGCAlgorithm.contains(gcAlg)) {
                fields.put("FullGCCount", GarbageCollectorMXBean.getCollectionCount());
                fields.put("FullGCTime", GarbageCollectorMXBean.getCollectionTime());
            } else {
                throw new RuntimeException("Unknown JVM gc Algorithm:" + gcAlg);
            }
            writer.write(DB, MEASURE_GC, getTags(host, identifier), fields);
        }
    }

    private static void collectOpenFD(String host, String identifier, MetricWriterStrategy writer) throws Throwable {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean) {
            Map<String, Object> fields = new HashMap<>();
            fields.put("openFileDescriptor", ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
            writer.write(DB, MEASURE_OPEN_FD, getTags(host, identifier), fields);
        }
    }

    private static void collectMem(String host, String identifier, MetricWriterStrategy writer) throws Throwable {
        MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();
        Map<String, Object> fields = new HashMap<>();
        fields.put("total.init", mxBean.getHeapMemoryUsage().getInit() + mxBean.getNonHeapMemoryUsage().getInit());
        fields.put("total.used", mxBean.getHeapMemoryUsage().getUsed() + mxBean.getNonHeapMemoryUsage().getUsed());
        fields.put("total.max", mxBean.getHeapMemoryUsage().getMax() + mxBean.getNonHeapMemoryUsage().getMax());
        fields.put("total.committed", mxBean.getHeapMemoryUsage().getCommitted() + mxBean.getNonHeapMemoryUsage().getCommitted());
        fields.put("heap.init", mxBean.getHeapMemoryUsage().getInit());
        fields.put("heap.used", mxBean.getHeapMemoryUsage().getUsed());
        fields.put("heap.max", mxBean.getHeapMemoryUsage().getMax());
        fields.put("heap.committed", mxBean.getHeapMemoryUsage().getCommitted());
        fields.put("non-heap.init", mxBean.getNonHeapMemoryUsage().getInit());
        fields.put("non-heap.used", mxBean.getNonHeapMemoryUsage().getUsed());
        fields.put("non-heap.max", mxBean.getNonHeapMemoryUsage().getMax());
        fields.put("non-heap.committed", mxBean.getNonHeapMemoryUsage().getCommitted());

        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            fields.put(getName(pool.getName()) + ".init", pool.getUsage().getInit());
            fields.put(getName(pool.getName()) + ".used", pool.getUsage().getUsed());
            fields.put(getName(pool.getName()) + ".max", pool.getUsage().getMax());
            fields.put(getName(pool.getName()) + ".committed", pool.getUsage().getCommitted());
        }
        writer.write(DB, MEASURE_MEM, getTags(host, identifier), fields);
    }

    private static String getName(String name) {
        String n;
        if (name.contains("Eden")) {
            n = "Eden Space";
        } else if (name.contains("Survivor")) {
            n = "Survivor Space";
        } else if (name.contains("Old") || name.contains("Tenured")) {
            n = "Old Gen";
        } else if (name.contains("Perm")) {
            n = "Perm Gen";
        } else {
            n = name;
        }
        return n;
    }

    private static Map<String, String> getTags(String host, String identifier) {
        Map<String, String> tags = new HashMap<>();
        tags.put("host", host);
        tags.put("identifier", identifier);
        return tags;
    }
}