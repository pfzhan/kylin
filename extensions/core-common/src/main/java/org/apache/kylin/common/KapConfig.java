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
