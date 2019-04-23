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

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.shaded.influxdb.org.influxdb.BatchOptions;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Point;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Pong;

public class InfluxDBWriter implements MetricWriter {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBWriter.class);

    public static final String DEFAULT_DATABASE = "KE_METRIC";
    public static final String DEFAULT_RP = "KE_METRIC_RP";

    private static volatile ScheduledExecutorService scheduledExecutorService;

    // easy to test
    static volatile InfluxDB influxDB;

    private static volatile InfluxDBWriter INSTANCE;

    private static volatile boolean isConnected = false;

    public static InfluxDBWriter getInstance() {
        if (INSTANCE == null) {
            synchronized (InfluxDBWriter.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }

                startMonitorInfluxDB();
                INSTANCE = new InfluxDBWriter();
            }
        }

        return INSTANCE;
    }

    private InfluxDBWriter() {
        tryConnectInfluxDB();

        getInfluxDB().setDatabase(DEFAULT_DATABASE);
        getInfluxDB().setRetentionPolicy(DEFAULT_RP);

        // enable async write. max batch size 1000, flush duration 3s.
        // when bufferLimit > actions，#RetryCapableBatchWriter will be used
        getInfluxDB().enableBatch(BatchOptions.DEFAULTS.actions(1000).bufferLimit(10000)
                .flushDuration(KapConfig.getInstanceFromEnv().getInfluxDBFlushDuration()).jitterDuration(500));

        if (!getInfluxDB().databaseExists(DEFAULT_DATABASE)) {
            logger.info("Create influxDB database {}", DEFAULT_DATABASE);
            getInfluxDB().createDatabase(DEFAULT_DATABASE);

            // create retention policy and use it as the default
            logger.info("Create influxDB retention policy '{}' on database '{}'", DEFAULT_RP, DEFAULT_DATABASE);
            getInfluxDB().createRetentionPolicy(DEFAULT_RP, DEFAULT_DATABASE, "30d", "7d", 1, true);
        }
    }

    private static void startMonitorInfluxDB() {
        if (scheduledExecutorService != null) {
            return;
        }

        logger.info("Start to monitor influxDB");
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("InfluxDBMon"));
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    tryConnectInfluxDB();

                    if (!Type.INFLUX.name().equals(System.getProperty("kap.metric.write-destination"))) {
                        // fallback to 'INFLUX', reset "kap.metric.write-destination" to "INFLUX"
                        logger.info("Changed to 'INFLUX'");
                        System.setProperty("kap.metric.write-destination", Type.INFLUX.name());
                    }

                } catch (Exception ex) {
                    logger.error("Monitor influxDB error", ex);
                }
            }
        }, 60, 60, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                getInfluxDB().close();
                scheduledExecutorService.shutdownNow();
                logger.info("Shutdown InfluxDB writer");
            }
        }));
    }

    private static void tryConnectInfluxDB() {
        try {
            final Pong pong = getInfluxDB().ping();
            logger.info("Connected to influxDB successfully. [{}]", pong);
            isConnected = true;
        } catch (Throwable th) {
            isConnected = false;
            throw new RuntimeException("Can not connected to influxDB", th);
        }
    }

    @Override
    public void write(String dbName, String measurement, Map<String, String> tags, Map<String, Object> fields,
            long timestamp) throws Throwable {
        if (!isConnected) {
            throw new IllegalStateException("Unable to got InfluxDB connection");
        }

        Point p = Point.measurement(measurement) //
                .time(timestamp, TimeUnit.MILLISECONDS) //
                .tag(tags) //
                .fields(fields) //
                .build(); //

        getInfluxDB().write(p);
    }

    private static InfluxDB getInfluxDB() {
        if (influxDB == null) {
            synchronized (InfluxDBWriter.class) {
                if (influxDB != null) {
                    return influxDB;
                }

                final String addr = KapConfig.getInstanceFromEnv().influxdbAddress();
                final String username = KapConfig.getInstanceFromEnv().influxdbUsername();
                final String password = KapConfig.getInstanceFromEnv().influxdbPassword();
                logger.info("Init influxDB, address: {}, username: {}, password: {}", addr, username, password);
                influxDB = InfluxDBFactory.connect("http://" + addr, username, password);
            }
        }

        return influxDB;
    }

    @Override
    public String getType() {
        return "INFLUX";
    }
}
