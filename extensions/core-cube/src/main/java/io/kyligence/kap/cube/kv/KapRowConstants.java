package io.kyligence.kap.cube.kv;

import org.apache.kylin.cube.kv.RowConstants;

/**
 * Created by roger on 5/30/16.
 */
public class KapRowConstants extends RowConstants{
    public static final byte ROWKEY_CUBEID_LEN= 16;
    public static final byte ROWKEY_SEGMENTID_LEN= 16;
    public static final byte ROWKEY_FOUR_IDS_LEN = ROWKEY_SHARD_AND_CUBOID_LEN +
            ROWKEY_CUBEID_LEN + ROWKEY_SEGMENTID_LEN;
}
