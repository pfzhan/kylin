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

package io.kyligence.kap.common.metrics;

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import io.kyligence.kap.shaded.influxdb.org.influxdb.BatchOptions;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;

public class NMetricsController {
    private static final Logger logger = LoggerFactory.getLogger(NMetricsController.class);

    private static final String KE_METRICS_RP = "KE_METRICS_RP";

    private static final AtomicBoolean reporterStarted = new AtomicBoolean(false);

    private static volatile MetricRegistry defaultMetricRegistry = null;

    private static volatile InfluxDB defaultInfluxDb = null;

    private NMetricsController() {
    }

    // try register metrics background
    // like: 1. NProjectManager.listAllProjects foreach register counters
    // 2. NDataModelManager.listAllModels register gauge
    // 3...

    public static MetricRegistry getDefaultMetricRegistry() {
        if (defaultMetricRegistry == null) {
            synchronized (NMetricsController.class) {
                if (defaultMetricRegistry == null) {
                    defaultMetricRegistry = new MetricRegistry();
                }
            }
        }
        return defaultMetricRegistry;
    }

    public static InfluxDB getDefaultInfluxDb() {

        if (defaultInfluxDb == null) {
            synchronized (NMetricsController.class) {
                if (defaultInfluxDb == null) {
                    final KapConfig config = KapConfig.getInstanceFromEnv();

                    defaultInfluxDb = InfluxDBFactory.connect(
                            new StringBuilder("http://").append(config.influxdbAddress()).toString(),
                            config.influxdbUsername(), config.influxdbPassword());

                    defaultInfluxDb.setDatabase(config.getMetricsDbNameWithMetadataUrlPrefix());
                    defaultInfluxDb.setRetentionPolicy(KE_METRICS_RP);
                    defaultInfluxDb.enableBatch(BatchOptions.DEFAULTS.actions(1000).bufferLimit(10000)
                            .flushDuration(config.getInfluxDBFlushDuration()).jitterDuration(500));

                    if (!defaultInfluxDb.databaseExists(config.getMetricsDbNameWithMetadataUrlPrefix())) {
                        defaultInfluxDb.createDatabase(config.getMetricsDbNameWithMetadataUrlPrefix());
                        defaultInfluxDb.createRetentionPolicy(KE_METRICS_RP,
                                config.getMetricsDbNameWithMetadataUrlPrefix(), "30d", "7d", 1, true);
                    }
                }
            }
        }
        return defaultInfluxDb;
    }

    public static void startReporters(KapConfig verifiableProps) {
        if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            return;
        }
        synchronized (reporterStarted) {
            if (!reporterStarted.get()) {
                try {
                    final NMetricsReporter influxDbReporter = new NMetricsInfluxdbReporter();
                    influxDbReporter.init(verifiableProps);

                    final JmxReporter jmxReporter = JmxReporter.forRegistry(getDefaultMetricRegistry()).build();
                    jmxReporter.start();

                    reporterStarted.set(true);

                    logger.info("ke.metrics reporters started");
                } catch (Exception e) {
                    logger.error("ke.metrics reporters start failed", e);
                }
            }
        }
    }

    static ClassLoader getClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        return cl == null ? NMetricsController.class.getClassLoader() : cl;
    }

    static <T> T createReporter(String reporterType, Object[] args) throws ClassNotFoundException,
            NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        if (args == null) {
            args = new Object[] {};
        }
        return (T) Class.forName(reporterType, true, getClassLoader())
                .getConstructor(Arrays.asList(args).stream().map(Object::getClass).toArray(Class[]::new))
                .newInstance(args);
    }

    static boolean registerMBean(Object mbean, String name)
            throws MalformedObjectNameException, InstanceNotFoundException, InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        synchronized (mbs) {
            ObjectName objName = new ObjectName(name);
            if (mbs.isRegistered(objName)) {
                mbs.unregisterMBean(objName);
            }
            mbs.registerMBean(mbean, objName);
            return true;
        }
    }
}
