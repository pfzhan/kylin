package io.kyligence.kap.cube.kv;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;

import java.nio.ByteBuffer;
import java.util.UUID;

public class KapRowKeyEncoder extends RowKeyEncoder {

    public KapRowKeyEncoder(CubeSegment cubeSeg, Cuboid cuboid) {
        super(cubeSeg, cuboid);
    }

    public int getHeaderLength() {
        return KapRowConstants.ROWKEY_FOUR_IDS_LEN;
    }

    public byte[] createBuf() {
        return new byte[this.getBytesLength()];
    }

    protected void fillHeader(byte[] bytes) {
        int offset = 0;

        // Fill cube id
        System.arraycopy(UuidBytes(cubeSeg.getCubeInstance().getUuid()), 0, bytes, offset, KapRowConstants.ROWKEY_CUBEID_LEN);
        offset += KapRowConstants.ROWKEY_CUBEID_LEN;

        // Fill segment id
        System.arraycopy(UuidBytes(cubeSeg.getUuid()), 0, bytes, offset, KapRowConstants.ROWKEY_SEGMENTID_LEN);
        offset += KapRowConstants.ROWKEY_SEGMENTID_LEN;

        // Fill cuboid id
        System.arraycopy(cuboid.getBytes(), 0, bytes, offset, RowConstants.ROWKEY_CUBOIDID_LEN);
        offset += KapRowConstants.ROWKEY_CUBOIDID_LEN;

        // Fill shard id
        short shard = calculateShard(bytes);
        BytesUtil.writeShort(shard, bytes, offset, KapRowConstants.ROWKEY_SHARDID_LEN);
    }

    private byte[] UuidBytes(String uuidString) {
        UUID uuid = UUID.fromString(uuidString);
        long hi = uuid.getMostSignificantBits();
        long lo = uuid.getLeastSignificantBits();
        return ByteBuffer.allocate(16).putLong(hi).putLong(lo).array();
    }
}

