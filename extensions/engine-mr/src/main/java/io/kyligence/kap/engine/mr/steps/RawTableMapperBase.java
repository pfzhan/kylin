package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.DataModelFlatTableDesc;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.raw.BufferedRawEncoder;

public class RawTableMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(RawTableMapperBase.class);
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String cubeName;
    protected String segmentID;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected List<byte[]> nullBytes;
    protected DataModelFlatTableDesc intermediateTableDesc;
    protected String intermediateTableRowDelimiter;
    protected byte byteRowDelimiter;
    protected Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected BytesSplitter bytesSplitter;
    private int errorRecordCounter;
    private RawTableInstance rawTableInstance;
    private RawTableDesc rawTableDesc;
    private BufferedRawEncoder rawEncoder;
    private String[] columnValues;
    protected int counter;
    protected Text outputKey = new Text();
    protected Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        intermediateTableRowDelimiter = context.getConfiguration().get(BatchConstants.CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER, Character.toString(BatchConstants.INTERMEDIATE_TABLE_ROW_DELIMITER));
        if (Bytes.toBytes(intermediateTableRowDelimiter).length > 1) {
            throw new RuntimeException("Expected delimiter byte length is 1, but got " + Bytes.toBytes(intermediateTableRowDelimiter).length);
        }
        byteRowDelimiter = Bytes.toBytes(intermediateTableRowDelimiter)[0];
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegmentById(segmentID);
        rawTableInstance = RawTableManager.getInstance(config).getRawTableInstance(cubeDesc.getName());
        rawTableDesc = rawTableInstance.getRawTableDesc();
        intermediateTableDesc = (DataModelFlatTableDesc) EngineFactory.getJoinedFlatTableDesc(cubeSegment);
        rawEncoder = new BufferedRawEncoder(rawTableDesc.getColumnsExcludingOrdered());
        columnValues = new String[rawTableDesc.getColumnsExcludingOrdered().size()];
        bytesSplitter = new BytesSplitter(200, 16384);
        dictionaryMap = cubeSegment.buildDictionaryMap();
        initNullBytes();
    }

    protected void outputKV(Context context) throws IOException, InterruptedException {
        intermediateTableDesc.sanityCheck(bytesSplitter);

        byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
        outputKey.set(rowKey, 0, rowKey.length);

        ByteBuffer valueBuf = buildValue(bytesSplitter.getSplitBuffers());
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(outputKey, outputValue);
    }

    protected byte[] buildKey(SplittedBytes[] splitBuffers) {
        TblColRef orderCol = rawTableDesc.getOrderedColumn();
        int index = intermediateTableDesc.getColumnIndex(orderCol);

        // TODO: get total shard num dynamically
        short shardId = ShardingHash.getShard(splitBuffers[index].value, 0, splitBuffers[index].length, 10);
        byte[] colValue = new byte[splitBuffers[index].length + 2];
        BytesUtil.writeShort(shardId, colValue, 0, 2);
        System.arraycopy(splitBuffers[index].value, 0, colValue, 2, splitBuffers[index].length);

        if (isNull(colValue)) {
            colValue = null;
        }
        return colValue;
    }

    protected ByteBuffer buildValue(SplittedBytes[] splitBuffers) {
        int i = 0;
        for (TblColRef col : rawTableDesc.getColumnsExcludingOrdered()) {
            int index = intermediateTableDesc.getColumnIndex(col);
            columnValues[i] = Bytes.toString(splitBuffers[index].value, 0, splitBuffers[index].length);
            i++;
        }
        return rawEncoder.encode(columnValues);
    }

    private void initNullBytes() {
        nullBytes = Lists.newArrayList();
        nullBytes.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullBytes.add(Bytes.toBytes(s));
            }
        }
    }

    protected boolean isNull(byte[] v) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, nullByte))
                return true;
        }
        return false;
    }

    protected byte[][] convertUTF8Bytes(String[] row) throws UnsupportedEncodingException {
        byte[][] result = new byte[row.length][];
        for (int i = 0; i < row.length; i++) {
            result[i] = row[i] == null ? HIVE_NULL : row[i].getBytes("UTF-8");
        }
        return result;
    }

    protected void handleErrorRecord(BytesSplitter bytesSplitter, Exception ex) throws IOException {

        logger.error("Insane record: " + bytesSplitter, ex);

        // TODO expose errorRecordCounter as hadoop counter
        errorRecordCounter++;
        if (errorRecordCounter > BatchConstants.ERROR_RECORD_LOG_THRESHOLD) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
