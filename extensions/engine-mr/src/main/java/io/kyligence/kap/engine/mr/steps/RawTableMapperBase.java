/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.DataModelFlatTableDesc;
import io.kyligence.kap.cube.raw.BufferedRawColumnCodec;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.cube.raw.RawValueIngester;
import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;
import io.kyligence.kap.cube.raw.gridtable.RawTableGridTable;

abstract public class RawTableMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(RawTableMapperBase.class);
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String rawTableName;
    protected String segmentID;
    protected CubeSegment cubeSegment;
    protected RawTableSegment rawSegment;
    protected List<byte[]> nullBytes;
    protected DataModelFlatTableDesc intermediateTableDesc;
    protected BytesSplitter bytesSplitter;
    private int errorRecordCounter;
    private RawTableInstance rawTableInstance;
    private RawTableDesc rawTableDesc;
    private BufferedRawColumnCodec rawColumnCodec;
    private String[] codecBuffer;
    private int[] shardbyColIdxInSource;
    private int[] shardbyColIdxInRawTable;
    private int orderColIdxInSource;
    private int orderColIdxInRawTable;
    private int[] nonOrderColInxInSource;// array index=> IdxInRawTable, array value=> IdxInSource

    protected Text outputKey = new Text();
    protected Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        rawTableName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        KapConfig kapConfig = KapConfig.wrap(config);

        rawTableInstance = RawTableManager.getInstance(config).getRawTableInstance(rawTableName);
        rawSegment = rawTableInstance.getSegmentById(segmentID);
        rawTableDesc = rawTableInstance.getRawTableDesc();
        cubeSegment = CubeManager.getInstance(config).getCube(rawTableName).getSegmentById(segmentID);
        intermediateTableDesc = (DataModelFlatTableDesc) EngineFactory.getJoinedFlatTableDesc(cubeSegment);

        GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableInstance);
        rawColumnCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());
        codecBuffer = new String[rawColumnCodec.getColumnsCount()]; // stored encoded value

        bytesSplitter = new BytesSplitter(kapConfig.getRawTableColumnCountMax(), kapConfig.getRawTableColumnLengthMax());
        initNullBytes();

        //pre-calculate index mappings for perf
        Collection<TblColRef> shardbyCols = rawTableDesc.getShardbyColumns();
        shardbyColIdxInSource = new int[shardbyCols.size()];
        shardbyColIdxInRawTable = new int[shardbyCols.size()];
        int index = 0;
        for (TblColRef shardColRef : shardbyCols) {
            shardbyColIdxInRawTable[index] = rawTableInstance.getRawToGridTableMapping().getIndexOf(shardColRef);
            shardbyColIdxInSource[index] = intermediateTableDesc.getColumnIndex(shardColRef);
            index++;
        }

        TblColRef orderCol = rawTableDesc.getOrderedColumn();//TODO: assuming only one sorted column here
        orderColIdxInSource = intermediateTableDesc.getColumnIndex(orderCol);
        orderColIdxInRawTable = rawTableInstance.getRawToGridTableMapping().getIndexOf(orderCol);
        nonOrderColInxInSource = new int[rawColumnCodec.getColumnsCount()];
        Arrays.fill(nonOrderColInxInSource, -1);
        for (TblColRef col : rawTableDesc.getColumnsExcludingOrdered()) {
            int sourceIdx = intermediateTableDesc.getColumnIndex(col);
            int rawTableIdx = rawTableInstance.getRawToGridTableMapping().getIndexOf(col);
            nonOrderColInxInSource[rawTableIdx] = sourceIdx;
        }
    }

    private void initNullBytes() {
        nullBytes = Lists.newArrayList();
        nullBytes.add(HIVE_NULL);
        String[] nullStrings = cubeSegment.getCubeDesc().getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullBytes.add(Bytes.toBytes(s));
            }
        }
    }

    protected boolean isNull(byte[] v, int offset, int length) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, offset, length, nullByte, 0, nullByte.length))
                return true;
        }
        return false;
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

        int shardNum = rawSegment.getShardNum() == 0 ? 2 : rawSegment.getShardNum();//by default 2 shards for raw table
        SplittedBytes orderedColSplittedBytes = splitBuffers[orderColIdxInSource];

        if (isNull(orderedColSplittedBytes.value, 0, orderedColSplittedBytes.length)) {
            codecBuffer[orderColIdxInRawTable] = null;
        } else {
            codecBuffer[orderColIdxInRawTable] = Bytes.toString(orderedColSplittedBytes.value, 0, orderedColSplittedBytes.length);
        }

        Object[] values = RawValueIngester.buildObjectOf(codecBuffer, rawColumnCodec, rawTableInstance.getRawToGridTableMapping().getOrderedColumnSet());
        ByteBuffer buffer = rawColumnCodec.encode(values, rawTableInstance.getRawToGridTableMapping().getOrderedColumnSet());

        //write body first
        int bodyLength = buffer.position();
        byte[] colValue = new byte[bodyLength + RowConstants.ROWKEY_SHARDID_LEN];
        System.arraycopy(buffer.array(), 0, colValue, RowConstants.ROWKEY_SHARDID_LEN, bodyLength);

        //then write shard
        ByteBuffer shardValue = getShardValue(splitBuffers);
        short shardId = ShardingHash.getShard(shardValue.array(), 0, shardValue.position(), shardNum);
        BytesUtil.writeShort(shardId, colValue, 0, RowConstants.ROWKEY_SHARDID_LEN);
        return colValue;
    }

    protected ByteBuffer getShardValue(SplittedBytes[] splitBuffers) {
        for (int i = 0; i < shardbyColIdxInSource.length; i++) {
            int idInSource = shardbyColIdxInSource[i];
            int idInRawTable = shardbyColIdxInRawTable[i];
            codecBuffer[idInRawTable] = Bytes.toString(splitBuffers[idInSource].value, 0, splitBuffers[idInSource].length);
        }
        Object[] values = RawValueIngester.buildObjectOf(codecBuffer, rawColumnCodec, rawTableInstance.getRawToGridTableMapping().getShardbyKey());
        return rawColumnCodec.encode(values, rawTableInstance.getRawToGridTableMapping().getShardbyKey());
    }

    protected ByteBuffer buildValue(SplittedBytes[] splitBuffers) {
        for (int i = 0; i < codecBuffer.length; i++) {
            if (nonOrderColInxInSource[i] != -1) {
                if (isNull(splitBuffers[nonOrderColInxInSource[i]].value, 0, splitBuffers[nonOrderColInxInSource[i]].length)) {
                    codecBuffer[i] = null;
                } else {
                    codecBuffer[i] = Bytes.toString(splitBuffers[nonOrderColInxInSource[i]].value, 0, splitBuffers[nonOrderColInxInSource[i]].length);
                }
            }
        }
        Object[] values = RawValueIngester.buildObjectOf(codecBuffer, rawColumnCodec, rawTableInstance.getRawToGridTableMapping().getNonOrderedColumnSet());
        return rawColumnCodec.encode(values, rawTableInstance.getRawToGridTableMapping().getNonOrderedColumnSet());
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
        if (errorRecordCounter > cubeSegment.getConfig().getErrorRecordThreshold()) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
