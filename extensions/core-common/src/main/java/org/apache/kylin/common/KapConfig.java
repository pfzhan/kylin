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
        return Integer.parseInt(config.getOptional("kap.parquet.ii.fuzzy.length", String.valueOf(6)));
    }

    public int getParquetIndexHashLength() {
        return Integer.parseInt(config.getOptional("kap.parquet.ii.hash.length", String.valueOf(8)));
    }

    public int getParquetRowsPerPage() {
        return Integer.parseInt(config.getOptional("kap.parquet.rows.per.page", String.valueOf(10000)));
    }

    public int getParquetPageIndexStepMax() {
        return Integer.parseInt(config.getOptional("kap.parquet.ii.step.max", String.valueOf(10000)));
    }

    public int getParquetPageIndexStepMin() {
        return Integer.parseInt(config.getOptional("kap.parquet.ii.step.min", String.valueOf(1000)));
    }

    public int getParquetPageIndexIOBufSize() {
        return Integer.parseInt(config.getOptional("kap.parquet.ii.io.buffer", String.valueOf(1024 * 1024)));
    }

    public String getParquetPageCompression() {
        return config.getOptional("kap.parquet.page.compression", "");
    }

    public int getParquetPageIndexSpillThreshold() {
        return Integer.parseInt(config.getOptional("kap.parquet.ii.spill.threshold", String.valueOf(100000)));
    }

    public String getSparkClientHost() {
        return config.getOptional("kap.parquet.spark.client.host", "localhost");
    }

    public int getSparkClientPort() {
        return Integer.parseInt(config.getOptional("kap.parquet.spark.client.port", "50051"));
    }

    public String getSparkCubeGTStorage() {
        return config.getOptional("kap.parquet.spark.cube.gtstorage", "io.kyligence.kap.storage.parquet.cube.CubeSparkRPC");
    }

    public String getSparkRawTableGTStorage() {
        return config.getOptional("kap.parquet.spark.rawtable.gtstorage", "io.kyligence.kap.storage.parquet.cube.raw.RawTableSparkRPC");
    }

    /**
     * where is parquet fles stored in hdfs , end with /
     */
    public String getParquentStoragePath() {
        String defaultPath = config.getHdfsWorkingDirectory() + "parquet/";
        return config.getOptional("kap.parquet.hdfs.dir", defaultPath);
    }

    /**
     * parquet shard size, in MB
     */
    public int getParquetStorageShardSize() {
        return Integer.valueOf(config.getOptional("kap.parquet.storage.shard.size", "64"));
    }

    public int getParquetStorageShardMin() {
        return Integer.valueOf(config.getOptional("kap.parquet.storage.shard.min", "1"));
    }

    public int getParquetStorageShardMax() {
        return Integer.valueOf(config.getOptional("kap.parquet.storage.shard.max", "1000"));
    }

    public int getParquetStorageBlockSize() {
        return Integer.valueOf(config.getOptional("kap.parquet.storage.dfs.blocksize", "134217728"));//default 128M
    }
}
