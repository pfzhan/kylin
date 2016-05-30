package io.kyligence.kap.common;

import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;

import java.io.InputStream;
import java.util.Map;

/**
 * Created by roger on 5/30/16.
 */
public class KapKylinConfig extends KylinConfig {

    // static cached instances
    private static KylinConfig ENV_INSTANCE = null;

    public static KylinConfig getInstanceFromEnv() {
        if (ENV_INSTANCE == null) {
            try {
                KylinConfig config = loadKylinConfig();
                ENV_INSTANCE = config;
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Failed to find KylinConfig ", e);
            }
        }
        return ENV_INSTANCE;
    }

    private static KylinConfig loadKylinConfig() {

        InputStream is = getKylinPropertiesAsInputStream();
        if (is == null) {
            throw new IllegalArgumentException("Failed to load kylin config");
        }
        KapKylinConfig config = new KapKylinConfig();
        config.reloadKylinConfig(is);

        return config;
    }

    public Map<Integer, String> getStorageEngines() {
        Map<Integer, String> r = convertKeyToInteger(getPropertiesByPrefix("kylin.storage.engine."));
        // ref constants in IStorageAware
        r.put(0, "org.apache.kylin.storage.hbase.HBaseStorage");
        r.put(1, "org.apache.kylin.storage.hybrid.HybridStorage");
        r.put(2, "org.apache.kylin.storage.hbase.HBaseStorage");
        r.put(3, "io.kyligence.kap.storage.parquet.ParquetStorage");
        return r;
    }

    private Map<Integer, String> convertKeyToInteger(Map<String, String> map) {
        Map<Integer, String> result = Maps.newLinkedHashMap();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result.put(Integer.valueOf(entry.getKey()), entry.getValue());
        }
        return result;
    }
}
