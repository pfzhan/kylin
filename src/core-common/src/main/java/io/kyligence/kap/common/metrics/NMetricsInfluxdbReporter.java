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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.kyligence.kap.common.metrics.service.InfluxDBInstance;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.metrics.reporter.InfluxdbReporter;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;

public class NMetricsInfluxdbReporter implements NMetricsReporter {

    private static final Logger logger = LoggerFactory.getLogger(NMetricsInfluxdbReporter.class);

    public static final String METRICS_MEASUREMENT = "system_metric";

    public static final String DAILY_METRICS_RETENTION_POLICY_NAME = "KE_METRICS_DAILY_RP";
    public static final String DAILY_METRICS_MEASUREMENT = "system_metric_daily";
    private AtomicInteger retry = new AtomicInteger(0);
    private AtomicLong lastUpdateTime = new AtomicLong(0);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final String reporterName = "MetricsReporter";

    private InfluxDB defaultInfluxDb = null;
    private String defaultMeasurement = null;
    private InfluxdbReporter underlying = null;
    private InfluxDBInstance influxDBInstance = null;

    private void updateDailyMetrics(long todayStart, NMetricsConfig config) {
        long yesterdayStart = TimeUtil.minusDays(todayStart, 1);
        this.underlying.getMetrics().forEach(point -> {
            StringBuilder sql = new StringBuilder("SELECT ");
            sql.append(StringUtils.join(point.getFields().keySet().stream()
                    .map(field -> String.format(" LAST(\"%s\") AS \"%s\" ", field, field)).collect(Collectors.toList()),
                    ","));
            sql.append(String.format(" FROM %s WHERE ", METRICS_MEASUREMENT));
            sql.append(String.format(" time >= %dms AND time < %dms ", yesterdayStart, todayStart));
            point.getTags().forEach((tag, value) -> sql.append(String.format(" AND %s='%s' ", tag, value)));

            QueryResult queryResult = this.underlying.getInfluxDb()
                    .query(new Query(sql.toString(), config.getMetricsDB()));

            if (CollectionUtils.isEmpty(queryResult.getResults())
                    || CollectionUtils.isEmpty(queryResult.getResults().get(0).getSeries())
                    || CollectionUtils.isEmpty(queryResult.getResults().get(0).getSeries().get(0).getValues())) {
                logger.warn("Failed to aggregate metric, cause query result is empty, uKey: {}!", point.getUniqueKey());
                return;
            }

            List<String> columns = queryResult.getResults().get(0).getSeries().get(0).getColumns();
            List<Object> firstLine = queryResult.getResults().get(0).getSeries().get(0).getValues().get(0);
            Map<String, Object> fields = Maps.newHashMap();
            for (int i = 1; i < columns.size(); i++) {
                fields.put(columns.get(i), firstLine.get(i));
            }

            influxDBInstance.write(config.getDailyMetricsDB(), DAILY_METRICS_MEASUREMENT, point.getTags(), fields,
                    yesterdayStart);
        });
    }

    private void startDailyReport(NMetricsConfig config) {
        influxDBInstance = new InfluxDBInstance(config.getDailyMetricsDB(), DAILY_METRICS_RETENTION_POLICY_NAME, "0d",
                "30d", 2, true);
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
            try {
                logger.info("Start to aggregate daily metrics ...");
                long now = System.currentTimeMillis();
                long todayStart = TimeUtil.getDayStart(now);

                // init and retry not check
                // lastUpdateTime < todayStart and config.getDailyMetricsRunHour() == TimeUtil.getHour(now) will run
                if (lastUpdateTime.get() > 0
                        && (retry.get() == 0 || retry.get() > config.getDailyMetricsMaxRetryTimes())) {
                    if (lastUpdateTime.get() > todayStart) {
                        return;
                    }

                    if (config.getDailyMetricsRunHour() != TimeUtil.getHour(now)) {
                        return;
                    }
                    retry.set(0);
                }

                // no metrics or metrics not
                if (CollectionUtils.isEmpty(this.underlying.getMetrics())) {
                    return;
                }

                lastUpdateTime.set(now);
                updateDailyMetrics(todayStart, config);

                retry.set(0);
                logger.info("Aggregate daily metrics success ...");
            } catch (Exception e) {
                retry.incrementAndGet();
                logger.error("Failed to aggregate daily metrics, retry: {}", retry.get(), e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

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
                startDailyReport(config);
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
            }
        }
    }

    @Override
    public String getMBeanName() {
        return "ke.metrics:type=NMetricsInfluxdbReporter";
    }
}
