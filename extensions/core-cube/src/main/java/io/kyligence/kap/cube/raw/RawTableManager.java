package io.kyligence.kap.cube.raw;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawTableManager implements IRealizationProvider {

    private static final Logger logger = LoggerFactory.getLogger(RawTableManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, RawTableManager> CACHE = new ConcurrentHashMap<KylinConfig, RawTableManager>();

    public static RawTableManager getInstance(KylinConfig config) {
        RawTableManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (RawTableManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new RawTableManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                    for (KylinConfig kylinConfig : CACHE.keySet()) {
                        logger.warn("type: " + kylinConfig.getClass() + " reference: " + System.identityHashCode(kylinConfig.base()));
                    }
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;

    public RawTableManager(KylinConfig config) throws IOException {
        this.config = config;
    }
    
    @Override
    public RealizationType getRealizationType() {
        return RealizationType.INVERTED_INDEX;
    }

    @Override
    public IRealization getRealization(String name) {
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cube = cubeMgr.getCube(name);
        if (cube != null && RawTableInstance.isRawTableEnabled(cube.getDescriptor())) {
            return new RawTableInstance(cube);
        }
        return null;
    }

    public RawTableInstance getRawTable(String name) {
        return (RawTableInstance) getRealization(name);
    }
}
