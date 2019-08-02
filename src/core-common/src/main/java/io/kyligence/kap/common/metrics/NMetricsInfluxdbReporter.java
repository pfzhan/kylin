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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.metrics.reporter.InfluxdbReporter;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;

public class NMetricsInfluxdbReporter implements NMetricsReporter {

    private static final Logger logger = LoggerFactory.getLogger(NMetricsInfluxdbReporter.class);

    public static final String METRICS_MEASUREMENT = "system_metric";

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final String reporterName = "MetricsReporter";

    private InfluxDB defaultInfluxDb = null;
    private String defaultMeasurement = null;
    private InfluxdbReporter underlying = null;

    @Override
    public void init(KapConfig kapConfig) {

        synchronized (this) {
            if (!initialized.get()) {
                final NMetricsConfig config = new NMetricsConfig(kapConfig);
                defaultMeasurement = METRICS_MEASUREMENT;
                defaultInfluxDb = NMetricsController.getDefaultInfluxDb();
                underlying = new InfluxdbReporter(defaultInfluxDb, defaultMeasurement,
                        NMetricsController.getDefaultMetricRegistry(), reporterName);
                initialized.set(true);
                startReporter(config.pollingIntervalSecs());
            }
        }
    }

    @Override
    public void startReporter(int pollingPeriodInSeconds) {
        synchronized (this) {
            if (initialized.get() && !running.get()) {
                underlying.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
                running.set(true);
                logger.info("ke.metrics influxdb reporter started");
            }
        }
    }

    @Override
    public void stopReporter() {
        synchronized (this) {
            if (initialized.get() && running.get()) {
                underlying.stop();
                underlying.close();
                running.set(false);
                underlying = new InfluxdbReporter(defaultInfluxDb, defaultMeasurement,
                        NMetricsController.getDefaultMetricRegistry(), reporterName);
            }
        }
    }

    @Override
    public String getMBeanName() {
        return "ke.metrics:type=NMetricsInfluxdbReporter";
    }
}
