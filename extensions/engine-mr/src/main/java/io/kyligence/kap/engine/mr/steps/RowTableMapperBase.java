package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableInstance;

/**
 * Created by wangcheng on 8/15/16.
 */
public class RowTableMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(RowTableMapperBase.class);
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String cubeName;
    protected String segmentID;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected List<byte[]> nullBytes;
    protected CubeJoinedFlatTableDesc intermediateTableDesc;
    protected String intermediateTableRowDelimiter;
    protected byte byteRowDelimiter;
    protected Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected BytesSplitter bytesSplitter;
    private int errorRecordCounter;
    private RawTableInstance rawTableInstance;
    protected byte[][] orderKeyBytesBuf;
    protected byte[][] valueBytesBuf;
    protected int orderKeyBytesLength;
    protected int valueBytesLength;
    protected int counter;
    protected Text outputKey = new Text();
    protected Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();

        logger.info("Cube Name: ****    " + cubeName);
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
        logger.error("*******" + segmentID);

        rawTableInstance = new RawTableInstance(cube);
        orderKeyBytesBuf = new byte[rawTableInstance.getRawTableDesc().getOrderedColumn().size()][];
        valueBytesBuf = new byte[rawTableInstance.getAllColumns().size()][];

        intermediateTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), cubeSegment);

        bytesSplitter = new BytesSplitter(200, 16384);
        dictionaryMap = cubeSegment.buildDictionaryMap();

        initNullBytes();
    }

    protected void outputKV(Context context) throws IOException, InterruptedException {
        intermediateTableDesc.sanityCheck(bytesSplitter);

        byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
        outputKey.set(rowKey, 0, rowKey.length);

        byte[] valueBuf = buildValue(bytesSplitter.getSplitBuffers());
        outputValue.set(valueBuf, 0, valueBuf.length);
        context.write(outputKey, outputValue);
    }

    protected byte[] buildKey(SplittedBytes[] splitBuffers) {
        int i = 0;
        for (ColumnDesc col : rawTableInstance.getRawTableDesc().getOrderedColumn()) {
            int indexAtHive = intermediateTableDesc.getColumnIndex(col.getRef());
            DimensionEncoding dimEnc = cubeSegment.getDimensionEncodingMap().get(col.getRef());

            byte[] colValue = Arrays.copyOf(splitBuffers[indexAtHive].value, splitBuffers[indexAtHive].length);

            if (isNull(colValue)) {
                orderKeyBytesBuf[i] = null;
                continue;
            }
            // if encode
            if (null != dimEnc) {
                byte[] bytes = new byte[dimEnc.getLengthOfEncoding()];
                orderKeyBytesLength += dimEnc.getLengthOfEncoding();
                dimEnc.encode(splitBuffers[indexAtHive].value, splitBuffers[indexAtHive].length, bytes, 0);
                orderKeyBytesBuf[i] = bytes;
            } else {
                orderKeyBytesBuf[i] = colValue;
                orderKeyBytesLength += splitBuffers[indexAtHive].length;
            }
            i++;
        }
        return encodeKey();
    }

    protected byte[] buildValue(SplittedBytes[] splitBuffers) {
        int i = 0;
        for (TblColRef col : rawTableInstance.getAllColumns()) {
            int indexAtHive = intermediateTableDesc.getColumnIndex(col);
            DimensionEncoding dimEnc = cubeSegment.getDimensionEncodingMap().get(col);
            // if encode
            if (null != dimEnc) {
                byte[] bytes = new byte[dimEnc.getLengthOfEncoding()];
                valueBytesLength += dimEnc.getLengthOfEncoding();
                dimEnc.encode(splitBuffers[indexAtHive].value, splitBuffers[indexAtHive].length, bytes, 0);
                valueBytesBuf[i] = bytes;
            } else {
                valueBytesBuf[i] = Arrays.copyOf(splitBuffers[indexAtHive].value, splitBuffers[indexAtHive].length);
                valueBytesLength += splitBuffers[indexAtHive].length;
            }
            i++;
        }
        return encodeValue();
    }

    protected byte[] encodeKey() {
        byte[] encodeBytes = new byte[orderKeyBytesLength];
        int nPos = 0;
        for (int i = 0; i < orderKeyBytesBuf.length; i++) {
            System.arraycopy(orderKeyBytesBuf[i], 0, encodeBytes, nPos, orderKeyBytesBuf[i].length);
            nPos += orderKeyBytesBuf[i].length;
        }
        return encodeBytes;
    }

    protected byte[] encodeValue() {
        byte[] encodeBytes = new byte[valueBytesLength];
        int nPos = 0;
        for (int i = 0; i < valueBytesBuf.length; i++) {
            System.arraycopy(valueBytesBuf[i], 0, encodeBytes, nPos, valueBytesBuf[i].length);
            nPos += orderKeyBytesBuf[i].length;
        }
        return encodeBytes;
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
