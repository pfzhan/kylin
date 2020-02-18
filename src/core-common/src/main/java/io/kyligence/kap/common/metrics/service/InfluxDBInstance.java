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

package io.kyligence.kap.common.metrics.service;

import com.google.common.base.Preconditions;
import io.kyligence.kap.shaded.influxdb.org.influxdb.BatchOptions;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Point;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Pong;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import lombok.Getter;
import lombok.val;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Getter
public class InfluxDBInstance {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBInstance.class);

    private String database;
    private String retentionPolicyName;
    private String retentionDuration;
    private String shardDuration;
    private int replicationFactor;
    private boolean useDefault;

    public static final String DEFAULT_DATABASE = "KE_MONITOR";
    private static final String DEFAULT_RETENTION_POLICY_NAME = "KE_MONITOR_RP";
    private static final String RETENTION_DURATION = "90d";
    private static final String SHARD_DURATION = "7d";
    private static final int REPLICATION_FACTOR = 1;
    private static final boolean USE_DEFAULT = true;

    private volatile InfluxDB influxDB;
    private volatile boolean connected = false;

    private ScheduledExecutorService scheduledExecutorService;
    private KylinConfig config;

    public static String generateDatabase(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrlPrefix() + "_" + KapConfig.wrap(kylinConfig).getMonitorDatabase();
    }

    public static String generateRetentionPolicy(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrlPrefix() + "_" + KapConfig.wrap(kylinConfig).getMonitorRetentionPolicy();
    }

    public InfluxDBInstance(KylinConfig kylinConfig) {
        this.config = kylinConfig;
        this.database = generateDatabase(kylinConfig);
        this.retentionPolicyName = generateRetentionPolicy(kylinConfig);
        this.retentionDuration = KapConfig.wrap(kylinConfig).getMonitorRetentionDuration();
        this.shardDuration = KapConfig.wrap(kylinConfig).getMonitorShardDuration();
        this.replicationFactor = KapConfig.wrap(kylinConfig).getMonitorReplicationFactor();
        this.useDefault = KapConfig.wrap(kylinConfig).isMonitorUserDefault();
        initInfluxDB();
    }

    public InfluxDBInstance(String database, String retentionPolicyName, String retentionDuration, String shardDuration,
            int replicationFactor, boolean useDefault) {
        this(null, database, retentionPolicyName, retentionDuration, shardDuration, replicationFactor, useDefault);
    }

    public InfluxDBInstance(InfluxDB influxDB, String database, String retentionPolicyName) {
        this(influxDB, database, retentionPolicyName, RETENTION_DURATION, SHARD_DURATION, REPLICATION_FACTOR,
                USE_DEFAULT);
    }

    public InfluxDBInstance(InfluxDB influxDB, String database, String retentionPolicyName, String retentionDuration,
            String shardDuration, int replicationFactor, boolean useDefault) {

        this.influxDB = influxDB;

        this.database = database;
        this.retentionPolicyName = retentionPolicyName;
        this.retentionDuration = retentionDuration;
        this.shardDuration = shardDuration;
        this.replicationFactor = replicationFactor;
        this.useDefault = useDefault;

        this.config = KylinConfig.getInstanceFromEnv();

        initInfluxDB();
    }

    private void initInfluxDB() {
        if (null == influxDB) {
            val kapConfig = KapConfig.wrap(config);
            final String addr = kapConfig.influxdbAddress();
            final String username = kapConfig.influxdbUsername();
            final String password = kapConfig.influxdbPassword();

            logger.info("Init influxDB, address: {}, username: {}", addr, username);

            influxDB = InfluxDBFactory.connect("http://" + addr, username, password);
        }

        checkDatabaseAndRetentionPolicy();
        startMonitorInfluxDB();
    }

    private void checkDatabaseAndRetentionPolicy() {
        tryConnectInfluxDB();

        getInfluxDB().setDatabase(getDatabase());
        getInfluxDB().setRetentionPolicy(getRetentionPolicyName());

        // enable async write. max batch size 1000, flush duration 3s.
        // when bufferLimit > actions，#RetryCapableBatchWriter will be used
        getInfluxDB().enableBatch(BatchOptions.DEFAULTS.actions(1000).bufferLimit(10000)
                .flushDuration(KapConfig.getInstanceFromEnv().getInfluxDBFlushDuration()).jitterDuration(500));

        if (!getInfluxDB().databaseExists(getDatabase())) {
            logger.info("Create influxDB database {}", getDatabase());
            getInfluxDB().createDatabase(getDatabase());
            // create retention policy and use it as the default
            logger.info("Create influxDB retention policy '{}' on database '{}'", getRetentionPolicyName(),
                    getDatabase());
            getInfluxDB().createRetentionPolicy(getRetentionPolicyName(), getDatabase(), getRetentionDuration(),
                    getShardDuration(), getReplicationFactor(), isUseDefault());
        }
    }

    private void tryConnectInfluxDB() {
        try {
            final Pong pong = getInfluxDB().ping();
            logger.trace("Connected to influxDB successfully. [{}]", pong);
            connected = true;
        } catch (Exception ex) {
            connected = false;
            throw ex;
        }
    }

    private void startMonitorInfluxDB() {
        logger.info("Start to monitor influxDB Instance, database: {}, retentionPolicy: {}", getDatabase(),
                getRetentionPolicyName());
        scheduledExecutorService = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("InfluxDBMonitor-" + this.getDatabase()));
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                tryConnectInfluxDB();
            } catch (Exception ex) {
                logger.error("Check influxDB Instance error, database: {}, retentionPolicy: {}", getDatabase(),
                        getRetentionPolicyName(), ex);
            }
        }, 60, 60, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            getInfluxDB().close();
            scheduledExecutorService.shutdownNow();
            logger.info("Shutdown InfluxDB Instance, database: {}, retentionPolicy: {}", getDatabase(),
                    getRetentionPolicyName());
        }));
    }

    public boolean write(String database, String measurement, Map<String, String> tags, Map<String, Object> fields,
            long timestamp) {
        if (!connected) {
            logger.error("InfluxDB is not connected, abort writing.");
            return false;
        }

        Preconditions.checkArgument(this.database.equals(database),
                "This InfluxDB Instance can not write with database: {}", database);

        Point p = Point.measurement(measurement) //
                .time(timestamp, TimeUnit.MILLISECONDS) //
                .tag(tags) //
                .fields(fields) //
                .build(); //

        getInfluxDB().write(p);
        return true;
    }

    public QueryResult read(String database, String sql) {
        Preconditions.checkArgument(this.database.equals(database),
                "This InfluxDB Instance can not read with database: {}", database);

        return getInfluxDB().query(new Query(sql, getDatabase()));
    }
}
