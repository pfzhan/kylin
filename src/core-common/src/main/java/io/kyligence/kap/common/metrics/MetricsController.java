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

import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.jmx.JmxReporter;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class MetricsController {
    private static final Logger logger = LoggerFactory.getLogger(MetricsController.class);


    private static final AtomicBoolean reporterStarted = new AtomicBoolean(false);

    private static volatile MetricRegistry defaultMetricRegistry = null;

    private MetricsController() {
    }

    // try register metrics background
    // like: 1. NProjectManager.listAllProjects foreach register counters
    // 2. NDataModelManager.listAllModels register gauge
    // 3...

    public static MetricRegistry getDefaultMetricRegistry() {
        if (defaultMetricRegistry == null) {
            synchronized (MetricsController.class) {
                if (defaultMetricRegistry == null) {
                    defaultMetricRegistry = new MetricRegistry();
                }
            }
        }
        return defaultMetricRegistry;
    }

    public static void startReporters(KapConfig verifiableProps) {
        if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            return;
        }
        synchronized (reporterStarted) {
            if (!reporterStarted.get()) {
                try {
                    final MetricsReporter influxDbReporter = MetricsInfluxdbReporter.getInstance();
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

}
