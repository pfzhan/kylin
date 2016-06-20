package io.kyligence.kap.common;

import org.apache.kylin.common.KylinConfig;

/**
 * Created by dong on 6/20/16.
 */
public class KAPKylinConfig extends KylinConfig {
    private KylinConfig config;

    // static cached instances
    private static KAPKylinConfig ENV_INSTANCE = null;

    public static KAPKylinConfig getInstanceFromEnv() {
        if (ENV_INSTANCE == null) {
            try {
                KAPKylinConfig config = wrapKylinConfig(KylinConfig.getInstanceFromEnv());
                ENV_INSTANCE = config;
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Failed to create KAPKylinConfig.", e);
            }
        }
        return ENV_INSTANCE;
    }

    public static KAPKylinConfig wrapKylinConfig(KylinConfig kylinConfig) {
        return new KAPKylinConfig(kylinConfig);
    }

    private KAPKylinConfig(KylinConfig config) {
        this.config = config;
    }

    public int getParquetRowsPerPage() {
        return Integer.parseInt(getOptional("kap.parquet.rows.per.page", String.valueOf(10000)));
    }
}
