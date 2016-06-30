package org.apache.kylin.common;

public class KapConfig {

    public static KapConfig getInstanceFromEnv() {
        return wrap(KylinConfig.getInstanceFromEnv());
    }

    public static KapConfig wrap(KylinConfig config) {
        return new KapConfig(config);
    }

    final private KylinConfig config;

    private KapConfig(KylinConfig config) {
        this.config = config;
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
}
