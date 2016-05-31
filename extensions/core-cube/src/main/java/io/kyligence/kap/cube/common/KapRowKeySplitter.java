package io.kyligence.kap.cube.common;

import io.kyligence.kap.cube.kv.KapRowConstants;
import org.apache.kylin.common.util.Bytes;

import java.nio.ByteBuffer;
import java.util.UUID;

public class KapRowKeySplitter {
    private String cubeId;
    private String segmentId;
    private long cuboidId;
    private short shardId;
    public static KapRowKeySplitter split(byte[] bytes) {
        KapRowKeySplitter splitter = new KapRowKeySplitter();
        splitter.cubeId = UuidString(bytes, 0, KapRowConstants.ROWKEY_CUBEID_LEN);
        splitter.segmentId = UuidString(bytes, KapRowConstants.ROWKEY_CUBEID_LEN,
                KapRowConstants.ROWKEY_SEGMENTID_LEN);
        splitter.cuboidId = Bytes.toLong(bytes,
                KapRowConstants.ROWKEY_CUBEID_LEN + KapRowConstants.ROWKEY_SEGMENTID_LEN,
                KapRowConstants.ROWKEY_CUBOIDID_LEN);
        splitter.shardId = Bytes.toShort(bytes,
                KapRowConstants.ROWKEY_CUBEID_LEN +
                        KapRowConstants.ROWKEY_SEGMENTID_LEN +
                        KapRowConstants.ROWKEY_CUBOIDID_LEN,
                KapRowConstants.ROWKEY_SHARDID_LEN);
        return splitter;
    }

    private static String UuidString(byte[] bytes, int offset, int length) {
        ByteBuffer bb = ByteBuffer.wrap(bytes, offset, length);
        long high = bb.getLong();
        long low = bb.getLong();
        UUID uuid = new UUID(high, low);
        return uuid.toString();
    }

    public String getCubeId() {
        return cubeId;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public long getCuboidId() {
        return cuboidId;
    }

    public short getShardId() {
        return shardId;
    }
}
