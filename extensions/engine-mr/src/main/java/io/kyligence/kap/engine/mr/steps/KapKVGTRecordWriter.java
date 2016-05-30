package io.kyligence.kap.engine.mr.steps;

import io.kyligence.kap.cube.kv.KapRowKeyEncoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.steps.KVGTRecordWriter;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.BufferedMeasureEncoder;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public abstract class KapKVGTRecordWriter implements ICuboidWriter {
    private static final Log logger = LogFactory.getLog(KVGTRecordWriter.class);
    private Long lastCuboidId;
    protected CubeSegment cubeSegment;
    protected CubeDesc cubeDesc;

    private AbstractRowKeyEncoder rowKeyEncoder;
    private int dimensions;
    private int measureCount;
    private byte[] keyBuf;
    private ImmutableBitSet measureColumns;
    private ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);
    private ByteArrayWritable outputKey = new ByteArrayWritable();
    private ByteArrayWritable outputValue = new ByteArrayWritable();
    private long cuboidRowCount = 0;

    //for shard

    public KapKVGTRecordWriter(CubeDesc cubeDesc, CubeSegment cubeSegment) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.measureCount = cubeDesc.getMeasures().size();
    }

    @Override
    public void write(long cuboidId, GTRecord record) throws IOException {

        if (lastCuboidId == null || !lastCuboidId.equals(cuboidId)) {
            if (lastCuboidId != null) {
                logger.info("Cuboid " + lastCuboidId + " has " + cuboidRowCount + " rows");
                cuboidRowCount = 0;
            }
            // output another cuboid
            initVariables(cuboidId);
            lastCuboidId = cuboidId;
        }

        cuboidRowCount++;
        rowKeyEncoder.encode(record, record.getInfo().getPrimaryKey(), keyBuf);

        //output measures
        valueBuf.clear();
        try {
            record.exportColumns(measureColumns, valueBuf);
        } catch (BufferOverflowException boe) {
            valueBuf = ByteBuffer.allocate((int) (record.sizeOf(measureColumns) * 1.5));
            record.exportColumns(measureColumns, valueBuf);
        }

        outputKey.set(keyBuf, 0, keyBuf.length);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        writeAsKeyValue(outputKey, outputValue);
    }

    protected abstract void writeAsKeyValue(ByteArrayWritable key, ByteArrayWritable value) throws IOException;

    private void initVariables(Long cuboidId) {
        rowKeyEncoder = KapRowKeyEncoderFactory.createInstance(cubeSegment, Cuboid.findById(cubeDesc, cuboidId));
        keyBuf = rowKeyEncoder.createBuf();

        dimensions = Long.bitCount(cuboidId);
        measureColumns = new ImmutableBitSet(dimensions, dimensions + measureCount);
    }
}
