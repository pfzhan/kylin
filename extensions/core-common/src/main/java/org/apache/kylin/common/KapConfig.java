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
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii.fuzzy.length", String.valueOf(6)));
    }

    public int getParquetFuzzyIndexHashLength() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii.fuzzy.hash.length", String.valueOf(32)));
    }

    public int getParquetIndexHashLength() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii.hash.length", String.valueOf(8)));
    }

    public int getParquetRowsPerPage() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.rows.per.page", String.valueOf(10000)));
    }

    public int getParquetPageIndexStepMax() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii.step.max", String.valueOf(10000)));
    }

    public int getParquetPageIndexStepMin() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii.step.min", String.valueOf(1000)));
    }

    public int getParquetPageIndexIOBufSize() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.ii.io.buffer", String.valueOf(1024 * 1024)));
    }

    public String getParquetPageCompression() {
        return config.getOptional("kap.storage.columnar.page.compression", "");
    }

    public double getParquetPageIndexSpillThresholdMB() {
        return Double.parseDouble(config.getOptional("kap.storage.columnar.ii.spill.threshold.mb", "128"));
    }

    public String getSparkClientHost() {
        return config.getOptional("kap.storage.columnar.spark.driver.host", "localhost");
    }

    public int getSparkClientPort() {
        return Integer.parseInt(config.getOptional("kap.storage.columnar.spark.driver.port", "7071"));
    }

    public String getSparkCubeGTStorage() {
        return config.getOptional("kap.storage.columnar.spark.cube.gtstorage", "io.kyligence.kap.storage.parquet.cube.CubeSparkRPC");
    }

    public String getSparkRawTableGTStorage() {
        return config.getOptional("kap.storage.columnar.spark.rawtable.gtstorage", "io.kyligence.kap.storage.parquet.cube.raw.RawTableSparkRPC");
    }

    public long getSparkVisitTimeout() {
        long value = Long.valueOf(config.getOptional("kap.storage.columnar.spark.visit.timeout", "300000"));
        return (long) (value * config.getCubeVisitTimeoutTimes());
    }

    /**
     * where is parquet fles stored in hdfs , end with /
     */
    public String getParquentStoragePath() {
        String defaultPath = config.getHdfsWorkingDirectory() + "parquet/";
        return config.getOptional("kap.storage.columnar.hdfs.dir", defaultPath);
    }

    /**
     * parquet shard size, in MB
     */
    public int getParquetStorageShardSize() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard.size", "64"));
    }

    public int getParquetStorageShardMin() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard.min", "1"));
    }

    public int getParquetStorageShardMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.shard.max", "1000"));
    }

    public int getParquetStorageBlockSize() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.dfs.blocksize", "134217728"));//default 128M
    }

    /**
     * Rawtable column
     */
    public int getRawTableColumnCountMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.rawtable.column.count.max", "3000"));
    }

    public int getRawTableColumnLengthMax() {
        return Integer.valueOf(config.getOptional("kap.storage.columnar.rawtable.column.length.max", "16384"));
    }

    /*
     * Cell level security config file
     */
    public String getCellLevelSecurityConfig() {
        return config.getOptional("kylin.cell.level.security.acl.config", "userctrl.acl");
    }

    public String getCellLevelSecurityEnable() {
        return config.getOptional("kylin.cell.level.security.enable", "false");
    }
}
