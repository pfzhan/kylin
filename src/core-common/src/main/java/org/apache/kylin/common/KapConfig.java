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

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

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

    // ============================================================================

    private final KylinConfig config;

    private static final String CIRCUIT_BREAKER_THRESHOLD = "30000";

    private static final String HALF_MINUTE_MS = "30000";

    private static final String FALSE = "false";

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

    public int getJdbcResourceStoreMaxCellSize() {
        return Integer.parseInt(config.getOptional("kap.metadata.jdbc.max-cell-size", "262144")); //256k
    }

    public boolean isParquetSeparateFsEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.storage.columnar.separate-fs-enable", FALSE));
    }

    public String getParquetReadFileSystem() {
        return config.getOptional("kylin.storage.columnar.file-system", "");
    }

    public int getParquetFuzzyIndexLength() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-fuzzy-length", String.valueOf(6)));
    }

    public int getParquetFuzzyIndexHashLength() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-fuzzy-hash-length", String.valueOf(32)));
    }

    public int getParquetIndexHashLength() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-hash-length", String.valueOf(8)));
    }

    public int getParquetRowsPerPage() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.rows-per-page", String.valueOf(10000)));
    }

    public int getParquetPagesPerGroup() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.pages-per-group", String.valueOf(4)));
    }

    public int getParquetRawWriterThresholdMB() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.writer-threshold-mb", String.valueOf(128)));
    }

    public int getParquetPageIndexStepMax() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-max-step", String.valueOf(10000)));
    }

    public int getParquetPageIndexStepMin() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-min-step", String.valueOf(1000)));
    }

    public int getParquetPageIndexIOBufSize() {
        return Integer
                .parseInt(config.getOptional("kap.storage.columnar.ii-io-buffer-bytes", String.valueOf(1024 * 1024)));
    }

    public int getParquetPageIndexMaxSeeks() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-max-seeks", String.valueOf(1024)));
    }

    public String getParquetPageCompression() {
        return config.getOptional("kap.storage.columnar.page-compression", "");
    }

    public double getParquetPageIndexSpillThresholdMB() {
        return Double.parseDouble(config.getOptional("kap.storage.columnar.ii-spill-threshold-mb", "128"));
    }

    public int getParquetSparkExecutorInstance() {
        return Integer.parseInt(
                config.getOptional("kap.storage.columnar.spark-conf.spark.executor.instances", String.valueOf(1)));
    }

    public int getParquetSparkExecutorCore() {
        return Integer.parseInt(
                config.getOptional("kap.storage.columnar.spark-conf.spark.executor.cores", String.valueOf(1)));
    }

    public boolean getParquetSparkDynamicResourceEnabled() {
        return Boolean
                .valueOf(config.getOptional("kap.storage.columnar.spark-conf.spark.dynamicAllocation.enabled", FALSE));
    }

    public int getParquetSparkExecutorInstanceMax() {
        return Integer.parseInt(config.getOptional(
                "kap.storage.columnar.spark-conf.spark.dynamicAllocation.maxExecutors", String.valueOf(1)));
    }

    public String getSparkClientHost() {
        return config.getOptional("kap.storage.columnar.spark-driver-host", "localhost");
    }

    public int getSparkClientPort() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.spark-driver-port", "7071"));
    }

    public int getGrpcMaxResponseSize() {
        return Integer.parseInt(
                config.getOptional("kap.storage.columnar.grpc-max-response-size", String.valueOf(128 * 1024 * 1024)));
    }

    public String getSparkCubeGTStorage() {
        return config.getOptional("kap.storage.columnar.spark-cube-gtstorage",
                "io.kyligence.kap.storage.parquet.cube.CubeSparkRPC");
    }

    public String getSparkRawTableGTStorage() {
        return config.getOptional("kap.storage.columnar.spark-rawtable-gtstorage",
                "io.kyligence.kap.storage.parquet.cube.raw.RawTableSparkRPC");
    }

    public long getSparkVisitTimeout() {
        return Long.valueOf(config.getOptional("kap.storage.columnar.spark-visit-timeout-ms", "300000"));
    }

    public int getAutoRepartitionRatio() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.auto-repartition-ratio", "3"));
    }

    public int getAutoRepartionThreshold() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.auto-repartition-threshold", "3"));
    }

    /**
     * where is parquet fles stored in hdfs , end with /
     */
    public String getWriteParquetStoragePath(String project) {
        String defaultPath = config.getHdfsWorkingDirectory() + project + "/parquet/";
        return config.getOptional("kap.storage.columnar.hdfs-dir", defaultPath);
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
    public int getParquetStorageShardSizeMB() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard-size-mb", "128"));
    }

    public long getParquetStorageShardSizeRowCount() {
        return Long.valueOf(config.getOptional("kap.storage.columnar.shard-rowcount", "2500000"));
    }

    public long getParquetStorageCountDistinctShardSizeRowCount() {
        return Long.valueOf(config.getOptional("kap.storage.columnar.shard-countdistinct-rowcount", "1000000"));
    }

    public int getParquetStorageRepartitionThresholdSize() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.repartition-threshold-size-mb", "128"));
    }

    public int getParquetStorageShardMin() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard-min", "1"));
    }

    public int getParquetStorageShardMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard-max", "1000"));
    }

    public int getParquetStorageBlockSize() {
        int defaultBlockSize = 5 * getParquetStorageShardSizeMB() * 1024 * 1024; //default (5 * shard_size)
        return Integer.valueOf(config.getOptional("kap.storage.columnar.hdfs-blocksize-bytes",
                String.valueOf(defaultBlockSize < 0 ? Integer.MAX_VALUE : defaultBlockSize)));
    }

    public int getParquetSpliceShardExpandFactor() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard-expand-factor", "10"));
    }

    public int getParquetDfsReplication() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.dfs-replication", "3"));
    }

    /**
     * Rawtable column
     */
    public int getRawTableColumnCountMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.rawtable-max-column-count", "30"));
    }

    public int getRawTableColumnLengthMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.rawtable-max-column-length", "16384"));
    }

    /**
     * query config
     */
    public boolean isUsingInvertedIndex() {
        return Boolean.valueOf(config.getOptional("kap.storage.columnar.ii-query-enabled", "true"));
    }

    public boolean isProjectInternalDefaultPermissionGranted() {
        return Boolean.parseBoolean(config.getOptional("kap.acl.project-internal-default-permission-granted", "true"));
    }

    /**
     * Massin
     */
    public String getMassinResourceIdentiferDir() {
        return config.getOptional("kap.server.massin-resource-dir", "/massin");
    }

    public String getZookeeperConnectString() {
        return config.getZookeeperConnectString();
    }

    public String getServerRestAddress() {
        return config.getOptional("kap.server.host-address", "localhost:7070");
    }

    /**
     * Sample Table
     */
    public long getViewMaterializeRowLimit() {
        return Long.parseLong(config.getOptional("kap.source.hive.tablestats.view-materialize-row-limit", "-1"));
    }

    /**
     * Diagnose Model
     */
    public long getJointDataSkewThreshold() {
        return Long.parseLong(config.getOptional("kap.source.hive.modelstats.joint-data-skew-threshold", "50000000"));
    }

    /**
     * Diagnosis config
     */
    public long getDiagPackageTimeout() {
        return Long.parseLong(config.getOptional(("kap.diag.package.timeout.seconds"), "3600"));
    }

    public int getExtractionStartTimeDays() {
        return Integer.parseInt(config.getOptional("kap.diag.extraction.start-time-days", "3"));
    }

    /**
     * Online service
     */
    public String getKyAccountUsename() {
        return config.getOptional("kap.kyaccount.username");
    }

    public String getKyAccountPassword() {
        return config.getOptional("kap.kyaccount.password");
    }

    public String getKyAccountToken() {
        return config.getOptional("kap.kyaccount.token");
    }

    public String getKyAccountSSOUrl() {
        return config.getOptional("kap.kyaccount.url", "https://sso.kyligence.com");
    }

    public String getKyAccountSiteUrl() {
        return config.getOptional("kap.kyaccount.site.url", "http://account.kyligence.io");
    }

    public String getKyBotSiteUrl() {
        return config.getOptional("kap.external.kybot.url", "https://kybot.io");
    }

    public String getHttpProxyHost() {
        return config.getOptional("kap.external.http.proxy.host");
    }

    public int getHttpProxyPort() {
        return Integer.parseInt(config.getOptional("kap.external.http.proxy.port", "-1"));
    }

    public String getChannelUser() {
        return config.getOptional("kap.channel.user", "on-premises");
    }

    /**
     * Spark configuration
     */
    public String getColumnarSparkEnv(String conf) {
        return config.getPropertiesByPrefix("kap.storage.columnar.spark-env.").get(conf);
    }

    public String getColumnarSparkConf(String conf) {
        return config.getPropertiesByPrefix("kap.storage.columnar.spark-conf.").get(conf);
    }

    public Map<String, String> getSparkConf() {
        return config.getPropertiesByPrefix("kap.storage.columnar.spark-conf.");
    }

    /**
     *  Advanced Flat Table
     */
    public boolean isAdvancedFlatTableByRowNum() {
        return getAdvancedFlatTableType().equals("rownum");
    }

    public String getAdvancedFlatTableType() {
        return config.getOptional("kap.job.advanced-flat-table.type", "percentage");
    }

    public int getAdvancedFlatTableRowNum() {
        return Integer.parseInt(config.getOptional("kap.job.advanced-flat-table.row-num", "1000"));
    }

    public int getAdvancedFlatTablePercentage() {
        return Integer.parseInt(config.getOptional("kap.job.advanced-flat-table.percentage", "10"));
    }

    /**
     *  Smart modeling
     */
    public String getSmartModelingConf(String conf) {
        return config.getOptional("kap.smart.conf." + conf, null);
    }

    public String getSmartModelingStrategy() {
        return config.getOptional("kap.smart.strategy", "default");
    }

    /**
     * Query
     */
    public int getCalciteJoinThreshold() {
        return Integer.parseInt(config.getOptional("kap.query.calcite-join-threshold", "-1"));
    }

    public boolean isRowACLEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.security.row-acl-enabled", "true"));
    }

    public boolean isColumnACLEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.security.column-acl-enabled", "true"));
    }

    public boolean isImplicitComputedColumnConvertEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.implicit-computed-column-convert", "true"));
    }

    public boolean isAggComputedColumnRewriteEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.agg-computed-column-rewrite", "true"));
    }

    public int getComputedColumnMaxRecursionTimes() {
        return Integer.valueOf(config.getOptional("kap.query.computed-column-max-recursion-times", "10"));
    }

    public boolean isJdbcEscapeEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.jdbc-escape-enabled", "true"));
    }

    public int getListenerBusBusyThreshold() {
        return Integer.valueOf(config.getOptional("kap.query.engine.spark-listenerbus-busy-threshold", "5000"));
    }

    public int getBlockNumBusyThreshold() {
        return Integer.valueOf(config.getOptional("kap.query.engine.spark-blocknum-busy-threshold", "5000"));
    }

    // in general, users should not set this, cuz we will auto calculate this num.
    public int getSparkSqlShufflePartitions() {
        return Integer.valueOf(config.getOptional("kap.query.engine.spark-sql-shuffle-partitions", "-1"));
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
    public String getMetadataDialect() {
        return config.getOptional("kylin.metadata.jdbc.dialect", "mysql");
    }

    public boolean isSparderEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.engine.sparder-enabled", "true"));
    }

    public boolean needReplaceAggWhenExactlyMatched() {
        return Boolean.valueOf(config.getOptional("kap.query.engine.need-replace-agg", "true"));
    }

    public String sparderFileFormat() {
        return config.getOptional("kap.query.engine.sparder-fileformat",
                "org.apache.spark.sql.execution.datasources.sparder.batch.SparderBatchFileFormat");
    }

    /**
     * health
     */

    public int getMetaStoreHealthWarningResponseMs() {
        return Integer.parseInt(config.getOptional("kap.health.metastore-warning-response-ms", "300"));
    }

    public int getMetaStoreHealthErrorResponseMs() {
        return Integer.parseInt(config.getOptional("kap.health.metastore-error-response-ms", "1000"));
    }

    /**
     * Diagnosis graph
     */
    public String getMetricWriteDest() {
        return config.getOptional("kap.metric.write-destination", "BLACK_HOLE");
    }

    public String influxdbAddress() {
        return config.getOptional("kap.influxdb.address", "localhost:8086");
    }

    public String influxdbUsername() {
        return config.getOptional("kap.influxdb.username", "root");
    }

    public String influxdbPassword() {
        return config.getOptional("kap.influxdb.password", "root");
    }

    public int getInfluxDBFlushDuration() {
        return Integer.valueOf(config.getOptional("kap.influxdb.flush-duration", "3000"));
    }

    public String sparderJars() {
        try {
            File storageFile = FileUtils.findFile(KylinConfigBase.getKylinHome() + "/lib", "newten-job.jar");
            String path1 = "";
            if (storageFile != null) {
                path1 = storageFile.getCanonicalPath();
            }

            return config.getOptional("kap.query.engine.sparder-additional-jars", path1);
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
        sb.append(config.getOptional("kap.metrics.influx.db", "KE_METRICS"));
        return sb.toString();
    }

    public String getMetricsRpcServiceBindAddress() {
        return config.getOptional("kap.metrics.influx.rpc-service.bind-address", "127.0.0.1:8088");
    }

    public int getMetricsPollingIntervalSecs() {
        return Integer.parseInt(config.getOptional("kap.metrics.polling.interval.secs", "60"));
    }

    /**
     * kap circuit-breaker
     */
    public Boolean isCircuitBreakerEnabled() {
        return Boolean.valueOf(config.getOptional("kap.circuit-breaker.enabled", "true"));
    }

    public int getCircuitBreakerThresholdOfProject() {
        return Integer.parseInt(config.getOptional("kap.circuit-breaker.threshold.project", "100"));
    }

    public int getCircuitBreakerThresholdOfModel() {
        return Integer.parseInt(config.getOptional("kap.circuit-breaker.threshold.model", "100"));
    }

    public int getCircuitBreakerThresholdOfFavoriteQuery() {
        return Integer.parseInt(config.getOptional("kap.circuit-breaker.threshold.fq", CIRCUIT_BREAKER_THRESHOLD));
    }

    public int getCircuitBreakerThresholdOfSqlPatternToBlacklist() {
        return Integer.parseInt(config.getOptional("kap.circuit-breaker.threshold.sql-pattern-to-blacklist",
                CIRCUIT_BREAKER_THRESHOLD));
    }

    public long getCircuitBreakerThresholdOfQueryResultRowCount() {
        return Long.parseLong(config.getOptional("kap.circuit-breaker.threshold.query-result-row-count", "2000000"));
    }

    /**
     * ZK Connection
     */
    public int getZKBaseSleepTimeMs() {
        return Integer.parseInt(config.getOptional("kap.env.zookeeper-base-sleep-time", "3000"));
    }

    public int getZKMaxRetries() {
        return Integer.parseInt(config.getOptional("kap.env.zookeeper-max-retries", "3"));
    }

    public int getMaxKeepLogFileNumber() {
        return Integer.parseInt(config.getOptional("kap.env.max-keep-log-file-number", "10"));
    }

    public int getMaxKeepLogFileThresholdMB() {
        return Integer.parseInt(config.getOptional("kap.env.max-keep-log-file-threshold-mb", "256"));
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
            return config.getOptional("kap.query.engine.sparder-additional-files", additionalFiles);
        } catch (IOException e) {
            return "";
        }
    }

    public String getAsyncResultBaseDir() {
        return config.getOptional("kap.query.engine.sparder-asyncresult-base-dir",
                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/async_query_result");
    }

    public Long getAsyncResultCleanUpInterval() {
        return Long.valueOf(config.getOptional("kap.query.engine.sparder-asyncresult-cleanup-interval",
                DateUtils.MILLIS_PER_DAY + ""));
    }

    public Boolean isAsyncResultRepartitionEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.engine.sparder-asyncresult-repartition-enabled", FALSE));
    }

    /**
     * Newten
     */
    public String getCuboidSpanningTree() {
        return config.getOptional("kap.cube.cuboid-spanning-tree",
                "io.kyligence.kap.metadata.cube.cuboid.NForestSpanningTree");
    }

    public float getSampleDatasetSizeRatio() {
        return Float.parseFloat(config.getOptional("kap.engine.spark-sample-dataset-ratio", "0.1f"));
    }

    public long getBuildDictionaryThreshold() {
        return Long.parseLong(config.getOptional("kap.engine.spark-build-dictionary-threshold", "15000000"));
    }

    public boolean needFetchAllSdictToExecutor() {
        return Boolean.valueOf(config.getOptional("kap.query.engine.fetch-sdict-to-executor", FALSE));
    }

    public String getParquetSeparateOverrideFiles() {
        return config.getOptional("kylin.storage.columnar.separate-override-files",
                "core-site.xml,hdfs-site.xml,yarn-site.xml");
    }

    public boolean enableQueryPattern() {
        return Boolean.valueOf(config.getOptional("kap.query.favorite.collect-as-pattern", "true"));
    }

    public boolean shouldMockMetadataWithoutDictStore() {
        return Boolean.valueOf(config.getOptional("kap.metadata.mock.no-dict-store", FALSE));
    }

    /**
     * Kerberos
     */

    public Boolean isKerberosEnabled() {
        return Boolean.valueOf(config.getOptional("kap.kerberos.enabled", FALSE));
    }

    public String getKerberosKeytab() {
        return config.getOptional("kap.kerberos.keytab", "");
    }

    public String getKerberosKeytabPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosKeytab();
    }

    public String getKerberosZKPrincipal() {
        return config.getOptional("kap.kerberos.zookeeper.server.principal", "zookeeper/hadoop");
    }

    public Long getKerberosTicketRefreshInterval() {
        return Long.valueOf(config.getOptional("kap.kerberos.ticket.refresh.interval.minutes", "720"));
    }

    public Long getKerberosMonitorInterval() {
        return Long.valueOf(config.getOptional("kap.kerberos.monitor.interval.minutes", "10"));
    }

    public String getKerberosPlatform() {
        return config.getOptional("kap.kerberos.platform", "");
    }

    public Boolean getPlatformZKEnable() {
        return Boolean.valueOf(config.getOptional("kap.platform.zk.kerberos.enable", FALSE));
    }

    public String getKerberosKrb5Conf() {
        return config.getOptional("kap.kerberos.krb5.conf", "krb5.conf");
    }

    public String getKerberosKrb5ConfPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosKrb5Conf();
    }

    public String getKerberosJaasConf() {
        return config.getOptional("kap.kerberos.jaas.conf", "jaas.conf");
    }

    public String getKerberosJaasConfPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosJaasConf();
    }

    public String getKerberosPrincipal() {
        return config.getOptional("kap.kerberos.principal");
    }

    public int getThresholdToRestartSpark() {
        return Integer.parseInt(config.getOptional("kap.canary.sqlcontext-threshold-to-restart-spark", "2"));
    }

    public int getSparkCanaryErrorResponseMs() {
        return Integer.parseInt(config.getOptional("kap.canary.sqlcontext-error-response-ms", HALF_MINUTE_MS));
    }

    public int getSparkCanaryPeriodMinutes() {
        return Integer.parseInt(config.getOptional("kap.canary.sqlcontext-period-min", "3"));
    }

    public double getJoinMemoryFraction() {
        // driver memory that can be used by join(mostly BHJ)
        return Double.parseDouble(config.getOptional("kap.query.join-memory-fraction", "0.3"));
    }

    public int getMonitorSparkPeriodSeconds() {
        return Integer.parseInt(config.getOptional("kap.storage.monitor-spark-period-seconds", "30"));
    }
}
