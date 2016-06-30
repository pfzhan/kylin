package org.apache.kylin.common;

public class KapConfig {

    // static cached instances
    private static KapConfig ENV_INSTANCE = null;

    public static KapConfig getInstanceFromEnv() {
        synchronized (KapConfig.class) {
            if (ENV_INSTANCE == null) {
                return wrap(KylinConfig.getInstanceFromEnv());
            }
            return ENV_INSTANCE;
        }
    }

    private static KapConfig wrap(KylinConfig config) {
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

    public String getParquetPageCompression() {
        return config.getOptional("kap.parquet.page.compression", "");
    }
}
