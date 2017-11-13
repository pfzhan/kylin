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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

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

    // ============================================================================

    final private KylinConfig config;

    private KapConfig(KylinConfig config) {
        this.config = config;
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
        return Boolean.parseBoolean(config.getOptional("kylin.storage.columnar.separate-fs-enable", "false"));
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
        return Boolean.valueOf(
                config.getOptional("kap.storage.columnar.spark-conf.spark.dynamicAllocation.enabled", "false"));
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

    public int getAutoRepartitionRatio(){
        return Integer.valueOf(config.getOptional("kap.storage.columnar.auto-repartition-ratio", "3"));
    }

    public int getAutoRepartionThreshold(){
        return Integer.valueOf(config.getOptional("kap.storage.columnar.auto-repartition-threshold", "3"));
    }
    /**
     * where is parquet fles stored in hdfs , end with /
     */
    public String getWriteParquetStoragePath() {
        String defaultPath = config.getHdfsWorkingDirectory() + "parquet/";
        return config.getOptional("kap.storage.columnar.hdfs-dir", defaultPath);
    }

    public String getReadParquetStoragePath() {
        if (StringUtils.isNotEmpty(getParquetReadFileSystem())) {
            Path parquetPath = new Path(getWriteParquetStoragePath());
            return new Path(getParquetReadFileSystem(), Path.getPathWithoutSchemeAndAuthority(parquetPath)).toString()
                    + "/";
        }

        return getWriteParquetStoragePath();
    }

    /**
     * parquet shard size, in MB
     */
    public int getParquetStorageShardSize() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard-size-mb", "256"));
    }

    public int getParquetStorageShardMin() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard-min", "1"));
    }

    public int getParquetStorageShardMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard-max", "1000"));
    }

    public int getParquetStorageBlockSize() {
        int defaultBlockSize = 5 * getParquetStorageShardSize() * 1024 * 1024; //default (5 * shard_size)
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

    public String getKyBotClientPath() {
        return config.getOptional("kybot.client.path", "kybot");
    }

    public String getHttpProxyHost() {
        return config.getOptional("kap.external.http.proxy.host");
    }

    public int getHttpProxyPort() {
        return Integer.parseInt(config.getOptional("kap.external.http.proxy.port", "-1"));
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

    public boolean isJdbcEscapeEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.jdbc-escape-enabled", "true"));
    }

    public boolean isCognosParenthesesEscapeEnabled() {
        return Boolean.valueOf(config.getOptional("kap.query.cognos-parentheses-escape", "false"));
    }
}
