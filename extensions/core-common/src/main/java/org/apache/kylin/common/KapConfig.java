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
            File confFile = KylinConfig.getKylinPropertiesFile();
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
        return Integer.parseInt(config.getOptional("kap.storage.columnar.pages-per-group", String.valueOf(10)));
    }

    public int getParquetPageIndexStepMax() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-max-step", String.valueOf(10000)));
    }

    public int getParquetPageIndexStepMin() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-min-step", String.valueOf(1000)));
    }

    public int getParquetPageIndexIOBufSize() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii-io-buffer-bytes", String.valueOf(1024 * 1024)));
    }

    public String getParquetPageCompression() {
        return config.getOptional("kap.storage.columnar.page-compression", "");
    }

    public double getParquetPageIndexSpillThresholdMB() {
        return Double.parseDouble(config.getOptional("kap.storage.columnar.ii-spill-threshold-mb", "128"));
    }

    public String getSparkClientHost() {
        return config.getOptional("kap.storage.columnar.spark-driver-host", "localhost");
    }

    public int getSparkClientPort() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.spark-driver-port", "7071"));
    }

    public String getSparkCubeGTStorage() {
        return config.getOptional("kap.storage.columnar.spark-cube-gtstorage", "io.kyligence.kap.storage.parquet.cube.CubeSparkRPC");
    }

    public String getSparkRawTableGTStorage() {
        return config.getOptional("kap.storage.columnar.spark-rawtable-gtstorage", "io.kyligence.kap.storage.parquet.cube.raw.RawTableSparkRPC");
    }

    public long getSparkVisitTimeout() {
        return Long.valueOf(config.getOptional("kap.storage.columnar.spark-visit-timeout-ms", "300000"));
    }

    /**
     * where is parquet fles stored in hdfs , end with /
     */
    public String getParquentStoragePath() {
        String defaultPath = config.getHdfsWorkingDirectory() + "parquet/";
        return config.getOptional("kap.storage.columnar.hdfs-dir", defaultPath);
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
        return Integer.valueOf(config.getOptional("kap.storage.columnar.hdfs-blocksize-bytes", String.valueOf(defaultBlockSize < 0 ? Integer.MAX_VALUE : defaultBlockSize)));
    }

    /**
     * Rawtable column
     */
    public int getRawTableColumnCountMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.rawtable-max-column-count", "3000"));
    }

    public int getRawTableColumnLengthMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.rawtable-max-column-length", "16384"));
    }

    /*
     * Cell level security config file
     */
    public String getCellLevelSecurityConfig() {
        return config.getOptional("kap.security.cell-level-acl-config", "userctrl.acl");
    }

    public String getCellLevelSecurityEnable() {
        return config.getOptional("kap.security.cell-level-acl-enabled", "false");
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
        return config.getOptional("kap.server.massin-resource-dir", new StringBuilder().append(config.getHdfsWorkingDirectory()).append("massin/").toString());
    }
    
    public String getHelixZookeeperAddress() {
        return config.getOptional("kap.job.helix.zookeeper-address");
    }

    public String getHelixRestAddress() {
        return config.getOptional("kap.job.helix.host-address", "localhost:7070");
    }

}
