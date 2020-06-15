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

package org.apache.kylin.common;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import io.kyligence.kap.common.util.EncryptUtil;
import io.kyligence.kap.common.util.FileUtils;

public class KapConfig {

    // no need to cache KapConfig as it is so lightweight
    public static KapConfig getInstanceFromEnv() {
        return wrap(KylinConfig.getInstanceFromEnv());
    }

    public static KapConfig wrap(KylinConfig config) {
        return new KapConfig(config);
    }

    public static File getKylinHomeAtBestEffort() {
        String kylinHome = KylinConfig.getKylinHome();
        if (kylinHome != null) {
            return new File(kylinHome).getAbsoluteFile();
        } else {
            File confFile = KylinConfig.getSitePropertiesFile();
            return confFile.getAbsoluteFile().getParentFile().getParentFile();
        }
    }

    public static String getKylinLogDirAtBestEffort() {
        return new File(getKylinHomeAtBestEffort(), "logs").getAbsolutePath();
    }

    public static String getKylinConfDirAtBestEffort() {
        return new File(getKylinHomeAtBestEffort(), "conf").getAbsolutePath();
    }

    // ============================================================================

    private final KylinConfig config;

    private static final String CIRCUIT_BREAKER_THRESHOLD = "30000";

    private static final String HALF_MINUTE_MS = "30000";

    private static final String FALSE = "false";

    private static final String TRUE = "true";

    public static final String FI_PLATFORM = "FI";

    private KapConfig(KylinConfig config) {
        this.config = config;
    }

    public KylinConfig getKylinConfig() {
        return config;
    }

    public boolean isDevEnv() {
        return config.isDevEnv();
    }

    public String getWriteHdfsWorkingDirectory() {
        return config.getHdfsWorkingDirectory();
    }

    public String getReadHdfsWorkingDirectory() {
        if (StringUtils.isNotEmpty(getParquetReadFileSystem())) {
            Path workingDir = new Path(getWriteHdfsWorkingDirectory());
            return new Path(getParquetReadFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString()
                    + "/";
        }

        return getWriteHdfsWorkingDirectory();
    }

    public String getParquetReadFileSystem() {
        return config.getOptional("kylin.storage.columnar.file-system", "");
    }

    public int getParquetSparkExecutorInstance() {
        return Integer.parseInt(
                config.getOptional("kylin.storage.columnar.spark-conf.spark.executor.instances", String.valueOf(1)));
    }

    public int getParquetSparkExecutorCore() {
        return Integer.parseInt(
                config.getOptional("kylin.storage.columnar.spark-conf.spark.executor.cores", String.valueOf(1)));
    }

    /**
     * where is parquet fles stored in hdfs , end with /
     */
    public String getWriteParquetStoragePath(String project) {
        String defaultPath = config.getHdfsWorkingDirectory() + project + "/parquet/";
        return config.getOptional("kylin.storage.columnar.hdfs-dir", defaultPath);
    }

    public String getReadParquetStoragePath(String project) {
        if (StringUtils.isNotEmpty(getParquetReadFileSystem())) {
            Path parquetPath = new Path(getWriteParquetStoragePath(project));
            return new Path(getParquetReadFileSystem(), Path.getPathWithoutSchemeAndAuthority(parquetPath)).toString()
                    + "/";
        }

        return getWriteParquetStoragePath(project);
    }

    /**
     * parquet shard size, in MB
     */
    public int getMinBucketsNumber() {
        return Integer.valueOf(config.getOptional("kylin.storage.columnar.bucket-num", "1"));
    }

    public int getParquetStorageShardSizeMB() {
        return Integer.valueOf(config.getOptional("kylin.storage.columnar.shard-size-mb", "128"));
    }

    public long getParquetStorageShardSizeRowCount() {
        return Long.valueOf(config.getOptional("kylin.storage.columnar.shard-rowcount", "2500000"));
    }

    public long getParquetStorageCountDistinctShardSizeRowCount() {
        return Long.valueOf(config.getOptional("kylin.storage.columnar.shard-countdistinct-rowcount", "1000000"));
    }

    public int getParquetStorageRepartitionThresholdSize() {
        return Integer.valueOf(config.getOptional("kylin.storage.columnar.repartition-threshold-size-mb", "128"));
    }

    public boolean isProjectInternalDefaultPermissionGranted() {
        return Boolean
                .parseBoolean(config.getOptional("kylin.acl.project-internal-default-permission-granted", "true"));
    }

    /**
     * Massin
     */
    public String getMassinResourceIdentiferDir() {
        return config.getOptional("kylin.server.massin-resource-dir", "/massin");
    }

    public String getZookeeperConnectString() {
        return config.getZookeeperConnectString();
    }

    public boolean getDBAccessFilterEnable() {
        return Boolean.parseBoolean(config.getOptional("kylin.source.hive.database-access-filter-enabled", "true"));
    }

    /**
     * Diagnosis config
     */
    public long getDiagPackageTimeout() {
        return Long.parseLong(config.getOptional(("kylin.diag.package.timeout-seconds"), "3600"));
    }

    public int getExtractionStartTimeDays() {
        return Integer.parseInt(config.getOptional("kylin.diag.extraction.start-time-days", "3"));
    }

    /**
     * Online service
     */
    public String getKyAccountUsename() {
        return config.getOptional("kylin.kyaccount.username");
    }

    public String getKyAccountPassword() {
        return config.getOptional("kylin.kyaccount.password");
    }

    public String getKyAccountToken() {
        return config.getOptional("kylin.kyaccount.token");
    }

    public String getKyAccountSSOUrl() {
        return config.getOptional("kylin.kyaccount.url", "https://sso.kyligence.com");
    }

    public String getKyAccountSiteUrl() {
        return config.getOptional("kylin.kyaccount.site-url", "http://account.kyligence.io");
    }

    public String getChannelUser() {
        return config.getOptional("kylin.env.channel", "on-premises");
    }

    /**
     * Spark configuration
     */
    public String getColumnarSparkEnv(String conf) {
        return config.getPropertiesByPrefix("kylin.storage.columnar.spark-env.").get(conf);
    }

    public String getColumnarSparkConf(String conf) {
        return config.getPropertiesByPrefix("kylin.storage.columnar.spark-conf.").get(conf);
    }

    public Map<String, String> getSparkConf() {
        return config.getPropertiesByPrefix("kylin.storage.columnar.spark-conf.");
    }

    /**
     *  Smart modeling
     */
    public String getSmartModelingConf(String conf) {
        return config.getOptional("kylin.smart.conf." + conf, null);
    }

    /**
     * Query
     */
    public boolean isImplicitComputedColumnConvertEnabled() {
        return Boolean.valueOf(config.getOptional("kylin.query.implicit-computed-column-convert", "true"));
    }

    public boolean isAggComputedColumnRewriteEnabled() {
        return Boolean.valueOf(config.getOptional("kylin.query.agg-computed-column-rewrite", "true"));
    }

    public int getComputedColumnMaxRecursionTimes() {
        return Integer.valueOf(config.getOptional("kylin.query.computed-column-max-recursion-times", "10"));
    }

    public boolean isJdbcEscapeEnabled() {
        return Boolean.valueOf(config.getOptional("kylin.query.jdbc-escape-enabled", "true"));
    }

    public int getListenerBusBusyThreshold() {
        return Integer.valueOf(config.getOptional("kylin.query.engine.spark-listenerbus-busy-threshold", "5000"));
    }

    public int getBlockNumBusyThreshold() {
        return Integer.valueOf(config.getOptional("kylin.query.engine.spark-blocknum-busy-threshold", "5000"));
    }

    // in general, users should not set this, cuz we will auto calculate this num.
    public int getSparkSqlShufflePartitions() {
        return Integer.valueOf(config.getOptional("kylin.query.engine.spark-sql-shuffle-partitions", "-1"));
    }

    /**
     * LDAP filter
     */
    public String getLDAPUserSearchFilter() {
        return config.getOptional("kylin.security.ldap.user-search-filter", "(objectClass=person)");
    }

    public String getLDAPGroupSearchFilter() {
        return config.getOptional("kylin.security.ldap.group-search-filter",
                "(|(objectClass=groupOfNames)(objectClass=group))");
    }

    public String getLDAPGroupMemberSearchFilter() {
        return config.getOptional("kylin.security.ldap.group-member-search-filter",
                "(&(cn={0})(objectClass=groupOfNames))");
    }

    public String getLDAPUserIDAttr() {
        return config.getOptional("kylin.security.ldap.user-identifier-attr", "cn");
    }

    public String getLDAPGroupIDAttr() {
        return config.getOptional("kylin.security.ldap.group-identifier-attr", "cn");
    }

    public String getLDAPGroupMemberAttr() {
        return config.getOptional("kylin.security.ldap.group-member-attr", "member");
    }

    /**
     * Metastore
     */
    public boolean isSparderEnabled() {
        return Boolean.valueOf(config.getOptional("kylin.query.engine.sparder-enabled", "true"));
    }

    public boolean needReplaceAggWhenExactlyMatched() {
        return Boolean.valueOf(config.getOptional("kylin.query.engine.need-replace-agg", "true"));
    }

    /**
     * health
     */

    public int getMetaStoreHealthWarningResponseMs() {
        return Integer.parseInt(config.getOptional("kylin.health.metastore-warning-response-ms", "300"));
    }

    public int getMetaStoreHealthErrorResponseMs() {
        return Integer.parseInt(config.getOptional("kylin.health.metastore-error-response-ms", "1000"));
    }

    /**
     * Diagnosis graph
     */

    public String influxdbAddress() {
        return config.getOptional("kylin.influxdb.address", "localhost:8086");
    }

    public String influxdbUsername() {
        return config.getOptional("kylin.influxdb.username", "root");
    }

    public String influxdbPassword() {
        String password = config.getOptional("kylin.influxdb.password", "root");
        if (EncryptUtil.isEncrypted(password)) {
            password = EncryptUtil.decryptPassInKylin(password);
        }
        return password;
    }

    public int getInfluxDBFlushDuration() {
        return Integer.valueOf(config.getOptional("kylin.influxdb.flush-duration", "3000"));
    }

    public String sparderJars() {
        try {
            File storageFile = FileUtils.findFile(KylinConfigBase.getKylinHome() + "/lib", "newten-job.jar");
            String path1 = "";
            if (storageFile != null) {
                path1 = storageFile.getCanonicalPath();
            }

            return config.getOptional("kylin.query.engine.sparder-additional-jars", path1);
        } catch (IOException e) {
            return "";
        }
    }

    /**
     * kap metrics, default: influxDb
     */
    public String getMetricsDbNameWithMetadataUrlPrefix() {
        StringBuilder sb = new StringBuilder(config.getMetadataUrlPrefix());
        sb.append("_");
        sb.append(config.getOptional("kylin.metrics.influx-db", "KE_METRICS"));
        return sb.toString();
    }

    public String getDailyMetricsDbNameWithMetadataUrlPrefix() {
        StringBuilder sb = new StringBuilder(config.getMetadataUrlPrefix());
        sb.append("_");
        sb.append(config.getOptional("kylin.metrics.daily-influx-db", "KE_METRICS_DAILY"));
        return sb.toString();
    }

    public String getMetricsRpcServiceBindAddress() {
        return config.getOptional("kylin.metrics.influx-rpc-service-bind-address", "127.0.0.1:8088");
    }

    public int getMetricsPollingIntervalSecs() {
        return Integer.parseInt(config.getOptional("kylin.metrics.polling-interval-secs", "60"));
    }

    public int getDailyMetricsRunHour() {
        return Integer.parseInt(config.getOptional("kylin.metrics.daily-run-hour", "1"));
    }

    public int getDailyMetricsMaxRetryTimes() {
        return Integer.parseInt(config.getOptional("kylin.metrics.daily-max-retry-times", "3"));
    }

    /**
     * kap monitor
     */
    public Boolean isMonitorEnabled() {
        return Boolean.valueOf(config.getOptional("kylin.monitor.enabled", "true"));
    }

    public String getMonitorDatabase() {
        return String.valueOf(config.getOptional("kylin.monitor.db", "KE_MONITOR"));
    }

    public String getMonitorRetentionPolicy() {
        return String.valueOf(config.getOptional("kylin.monitor.retention-policy", "KE_MONITOR_RP"));
    }

    public String getMonitorRetentionDuration() {
        return String.valueOf(config.getOptional("kylin.monitor.retention-duration", "90d"));
    }

    public String getMonitorShardDuration() {
        return String.valueOf(config.getOptional("kylin.monitor.shard-duration", "7d"));
    }

    public Integer getMonitorReplicationFactor() {
        return Integer.valueOf(config.getOptional("kylin.monitor.replication-factor", "1"));
    }

    public Boolean isMonitorUserDefault() {
        return Boolean.valueOf(config.getOptional("kylin.monitor.user-default", "true"));
    }

    public Long getMonitorInterval() {
        return Long.valueOf(config.getOptional("kylin.monitor.interval", "60")) * 1000;
    }

    public long getJobStatisticInterval() {
        return Long.valueOf(config.getOptional("kylin.monitor.job-statistic-interval", "3600")) * 1000;
    }

    public long getMaxPendingErrorJobs() {
        return Long.valueOf(config.getOptional("kylin.monitor.job-pending-error-total", "20"));
    }

    public double getMaxPendingErrorJobsRation() {
        double ration = Double.parseDouble(config.getOptional("kylin.monitor.job-pending-error-rate", "0.2"));
        if (ration <= 0 || ration >= 1) {
            return 0.2;
        }
        return ration;
    }

    public double getClusterCrashThreshhold() {
        return Double.parseDouble(config.getOptional("kylin.monitor.cluster-crash-threshold", "0.8"));
    }

    /**
     * kap circuit-breaker
     */
    public Boolean isCircuitBreakerEnabled() {
        return Boolean.valueOf(config.getOptional("kylin.circuit-breaker.enabled", "true"));
    }

    public int getCircuitBreakerThresholdOfProject() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.project", "100"));
    }

    public int getCircuitBreakerThresholdOfModel() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.model", "100"));
    }

    public int getCircuitBreakerThresholdOfFavoriteQuery() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.fq", CIRCUIT_BREAKER_THRESHOLD));
    }

    public int getCircuitBreakerThresholdOfSqlPatternToBlacklist() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.sql-pattern-to-blacklist",
                CIRCUIT_BREAKER_THRESHOLD));
    }

    public long getCircuitBreakerThresholdOfQueryResultRowCount() {
        return Long.parseLong(config.getOptional("kylin.circuit-breaker.threshold.query-result-row-count", "2000000"));
    }

    // log rotate
    public int getMaxKeepLogFileNumber() {
        return Integer.parseInt(config.getOptional("kylin.env.max-keep-log-file-number", "10"));
    }

    public int getMaxKeepLogFileThresholdMB() {
        return Integer.parseInt(config.getOptional("kylin.env.max-keep-log-file-threshold-mb", "256"));
    }

    public String sparderFiles() {
        try {
            File storageFile = new File(getKylinConfig().getLogSparkExecutorPropertiesFile());
            String additionalFiles = storageFile.getCanonicalPath();
            storageFile = new File(getKylinConfig().getLogSparkAppMasterPropertiesFile());
            if (additionalFiles.isEmpty()) {
                additionalFiles = storageFile.getCanonicalPath();
            } else {
                additionalFiles = additionalFiles + "," + storageFile.getCanonicalPath();
            }
            return config.getOptional("kylin.query.engine.sparder-additional-files", additionalFiles);
        } catch (IOException e) {
            return "";
        }
    }

    public String getAsyncResultBaseDir() {
        return config.getOptional("kylin.query.engine.sparder-asyncresult-base-dir",
                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/async_query_result");
    }

    /**
     * Newten
     */
    public String getCuboidSpanningTree() {
        return config.getOptional("kylin.cube.cuboid-spanning-tree",
                "io.kyligence.kap.metadata.cube.cuboid.NForestSpanningTree");
    }

    public String getIntersectCountSeparator() {
        return config.getOptional("kylin.cube.intersect-count-array-separator", "|");
    }

    public boolean enableQueryPattern() {
        return Boolean.valueOf(config.getOptional("kylin.query.favorite.collect-as-pattern", "true"));
    }

    public boolean splitGroupSetsIntoUnion() {
        return Boolean.valueOf(config.getOptional("kylin.query.engine.split-group-sets-into-union", TRUE));
    }

    public int defaultDecimalScale() {
        return Integer.valueOf(config.getOptional("kylin.query.engine.default-decimal-scale", "0"));
    }

    public boolean enablePushdownPrepareStatementWithParams() {
        return Boolean.valueOf(
                config.getOptional("kylin.query.engine.push-down.enable-prepare-statement-with-params", FALSE));
    }

    public boolean runConstantQueryLocally() {
        return Boolean.valueOf(config.getOptional("kylin.query.engine.run-constant-query-locally", TRUE));
    }

    public boolean isRecordSourceUsage() {
        return Boolean.parseBoolean(config.getOptional("kylin.source.record-source-usage-enabled", "true"));
    }

    /**
     * Kerberos
     */

    public Boolean isKerberosEnabled() {
        return Boolean.valueOf(config.getOptional("kylin.kerberos.enabled", FALSE));
    }

    public String getKerberosKeytab() {
        return config.getOptional("kylin.kerberos.keytab", "");
    }

    public String getKerberosKeytabPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosKeytab();
    }

    public String getKerberosZKPrincipal() {
        return config.getOptional("kylin.kerberos.zookeeper-server-principal", "zookeeper/hadoop");
    }

    public Long getKerberosTicketRefreshInterval() {
        return Long.valueOf(config.getOptional("kylin.kerberos.ticket-refresh-interval-minutes", "720"));
    }

    public Long getKerberosMonitorInterval() {
        return Long.valueOf(config.getOptional("kylin.kerberos.monitor-interval-minutes", "10"));
    }

    public String getKerberosPlatform() {
        return config.getOptional("kylin.kerberos.platform", "");
    }

    public Boolean getPlatformZKEnable() {
        return Boolean.valueOf(config.getOptional("kylin.env.zk-kerberos-enabled", FALSE));
    }

    public String getKerberosKrb5Conf() {
        return config.getOptional("kylin.kerberos.krb5-conf", "krb5.conf");
    }

    public String getKerberosKrb5ConfPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosKrb5Conf();
    }

    public String getKerberosJaasConf() {
        return config.getOptional("kylin.kerberos.jaas-conf", "jaas.conf");
    }

    public String getKerberosJaasConfPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosJaasConf();
    }

    public String getKerberosPrincipal() {
        return config.getOptional("kylin.kerberos.principal");
    }

    public int getThresholdToRestartSpark() {
        return Integer.parseInt(config.getOptional("kylin.canary.sqlcontext-threshold-to-restart-spark", "3"));
    }

    public int getSparkCanaryErrorResponseMs() {
        return Integer.parseInt(config.getOptional("kylin.canary.sqlcontext-error-response-ms", HALF_MINUTE_MS));
    }

    public int getSparkCanaryPeriodMinutes() {
        return Integer.parseInt(config.getOptional("kylin.canary.sqlcontext-period-min", "3"));
    }

    public double getJoinMemoryFraction() {
        // driver memory that can be used by join(mostly BHJ)
        return Double.parseDouble(config.getOptional("kylin.query.join-memory-fraction", "0.3"));
    }

    public int getMonitorSparkPeriodSeconds() {
        return Integer.parseInt(config.getOptional("kylin.storage.monitor-spark-period-seconds", "30"));
    }

    public boolean isQueryEscapedLiteral() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.parser.escaped-string-literals", FALSE));
    }
}
