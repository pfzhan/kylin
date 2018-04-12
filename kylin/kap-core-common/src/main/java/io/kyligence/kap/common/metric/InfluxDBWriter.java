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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Point;

public enum InfluxDBWriter implements MetricWriter {
    INSTANCE;
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBWriter.class);
    private static volatile InfluxDB influxDB;
    private static volatile ScheduledExecutorService scheduledExecutorService;

    private static InfluxDB getInfluxDBIns() throws UnknownHostException {
        if (influxDB == null) {
            synchronized (InfluxDBWriter.class) {
                if (influxDB == null) {
                    String addr = System.getProperty("influxDB.address");
                    if (StringUtils.isBlank(addr)) {
                        addr = InetAddress.getLocalHost().getHostName() + ":8085";
                    }
                    logger.info("The influx DB' address is:" + addr);
                    influxDB = InfluxDBFactory.connect("http://" + addr, "root", "root");
                    logger.info("Create influxDB Database KAP_METRIC");
                    influxDB.createDatabase("KAP_METRIC");
                    influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
                    logger.info(influxDB.ping().toString());
                    monitorInfluxDB();
                }
            }
        }
        return influxDB;
    }

    private static void monitorInfluxDB() {
        if (scheduledExecutorService == null) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            logger.info("Start to ping influxDB");
            scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (influxDB != null) {
                            logger.info("ping influxDB");
                            influxDB.ping();
                        }
                    } catch (Throwable th) {
                        logger.info("lost influxDB, close existing instance.");
                        influxDB.close();
                        influxDB = null;
                    }
                }
            }, 10, 60, TimeUnit.SECONDS);
        }
    }

    public void write(String dbName, String measurement, Map<String, String> tags, Map<String, Object> fields) throws Throwable {
        logger.trace("Start to send to influx DB");
        InfluxDB instance = getInfluxDBIns();
        if (instance == null) {
            return;
        }

        Point p = Point.measurement(measurement) //
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS) //
                .tag(tags) //
                .fields(fields) //
                .build(); //
        instance.write(dbName, "autogen", p);
        logger.trace("finished to send to influx DB");
    }

    @Override
    public String getType() {
        return "INFLUX";
    }
}
