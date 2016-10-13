/**
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
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.DataModelFlatTableDesc;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.raw.BufferedRawEncoder;

public class RawTableMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(RawTableMapperBase.class);
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String rawTableName;
    protected String segmentID;
    protected CubeSegment cubeSegment;
    protected RawTableSegment rawSegment;
    protected DataModelFlatTableDesc intermediateTableDesc;
    protected BytesSplitter bytesSplitter;
    private int errorRecordCounter;
    private RawTableInstance rawInstance;
    private RawTableDesc rawTableDesc;
    private BufferedRawEncoder rawEncoder;
    private BufferedRawEncoder orderedEncoder;

    private String[] columnValues;
    protected int counter;
    protected Text outputKey = new Text();
    protected Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        rawTableName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        KapConfig kapConfig = KapConfig.wrap(config);

        rawInstance = RawTableManager.getInstance(config).getRawTableInstance(rawTableName);
        rawSegment = rawInstance.getSegmentById(segmentID);
        rawTableDesc = rawInstance.getRawTableDesc();
        cubeSegment = CubeManager.getInstance(config).getCube(rawTableName).getSegmentById(segmentID);
        intermediateTableDesc = (DataModelFlatTableDesc) EngineFactory.getJoinedFlatTableDesc(cubeSegment);
        rawEncoder = new BufferedRawEncoder(rawTableDesc.getColumnsExcludingOrdered());
        orderedEncoder = new BufferedRawEncoder(rawTableDesc.getOrderedColumn());
        columnValues = new String[rawTableDesc.getColumnsExcludingOrdered().size()];
        bytesSplitter = new BytesSplitter(kapConfig.getRawTableColumnCountMax(), kapConfig.getRawTableColumnLengthMax());
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

        int shardNum = rawSegment.getShardNum() == 0 ? 10 : rawSegment.getShardNum();
        short shardId = ShardingHash.getShard(splitBuffers[index].value, 0, splitBuffers[index].length, shardNum);
        String[] orderString = new String[] { Bytes.toString(splitBuffers[index].value, 0, splitBuffers[index].length) };
        ByteBuffer buffer = orderedEncoder.encode(orderString);
        byte[] colValue = new byte[buffer.position() + 2];
        BytesUtil.writeShort(shardId, colValue, 0, 2);
        System.arraycopy(buffer.array(), 0, colValue, 2, buffer.position());
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
