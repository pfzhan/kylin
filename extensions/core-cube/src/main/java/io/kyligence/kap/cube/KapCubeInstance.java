package io.kyligence.kap.cube;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.cube.CubeInstance;

/**
 * Created by roger on 5/28/16.
 */
public class KapCubeInstance extends CubeInstance{
    private KylinConfigExt config;

    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public KapCubeInstance(CubeInstance cubeInstance) {
        super.setName(cubeInstance.getName());
        super.setSegments(cubeInstance.getSegments());
        super.setDescName(cubeInstance.getDescName());
        super.setStatus(cubeInstance.getStatus());
        super.setOwner(cubeInstance.getOwner());
        super.setCost(cubeInstance.getCost());
        super.setCreateTimeUTC(System.currentTimeMillis());
        super.updateRandomUuid();

        config = (KylinConfigExt) cubeInstance.getConfig();
    }
}
