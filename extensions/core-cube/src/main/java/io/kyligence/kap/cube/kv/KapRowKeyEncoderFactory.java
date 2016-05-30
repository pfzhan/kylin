package io.kyligence.kap.cube.kv;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;

/**
 * Created by roger on 5/27/16.
 */
public abstract  class KapRowKeyEncoderFactory {
    public static AbstractRowKeyEncoder createInstance(CubeSegment cubeSeg, Cuboid cuboid) {
        return new KapRowKeyEncoder(cubeSeg, cuboid);
    }
}
