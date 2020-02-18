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

import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.common.metrics.service.InfluxDBInstance;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBWriter implements MetricWriter {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBWriter.class);

    public static final String DEFAULT_DATABASE = "KE_HISTORY";
    public static final String DEFAULT_RP = "KE_HISTORY_RP";
    private static final String RETENTION_DURATION = "30d";
    private static final String SHARD_DURATION = "7d";
    private static final int REPLICATION_FACTOR = 1;
    private static final boolean USE_DEFAULT = true;

    // easy to test
    static volatile InfluxDB influxDB;

    private static volatile InfluxDBWriter INSTANCE;

    @Getter
    private InfluxDBInstance influxDBInstance;

    public static InfluxDBWriter getInstance() {
        if (INSTANCE == null) {
            synchronized (InfluxDBWriter.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }

                // easy to test
                if (null != influxDB) {
                    INSTANCE = new InfluxDBWriter(new InfluxDBInstance(influxDB, DEFAULT_DATABASE, DEFAULT_RP,
                            RETENTION_DURATION, SHARD_DURATION, REPLICATION_FACTOR, USE_DEFAULT));
                } else {
                    INSTANCE = new InfluxDBWriter(new InfluxDBInstance(DEFAULT_DATABASE, DEFAULT_RP, RETENTION_DURATION,
                            SHARD_DURATION, REPLICATION_FACTOR, USE_DEFAULT));
                }
            }
        }

        return INSTANCE;
    }

    private InfluxDBWriter(InfluxDBInstance influxDBInstance) {
        this.influxDBInstance = influxDBInstance;
    }

    @Override
    public void write(String dbName, String measurement, Map<String, String> tags, Map<String, Object> fields,
            long timestamp) throws Throwable {
        this.influxDBInstance.write(dbName, measurement, tags, fields, timestamp);
    }

    @Override
    public String getType() {
        return "INFLUX";
    }
}
