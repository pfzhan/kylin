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

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
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
import io.kyligence.kap.cube.raw.gridtable.RawToGridTableMapping;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;

abstract public class RawTableMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, ByteArrayListWritable, ByteArrayListWritable> {
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
    protected ByteArrayListWritable outputKey;
    protected ByteArrayListWritable outputValue;
    private Pair<Integer, Integer>[] shardbyMapping;
    private Pair<Integer, Integer>[] sortbyMapping;
    private Pair<Integer, Integer>[] nonSortbyMapping;

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

        GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableDesc);
        rawColumnCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());
        codecBuffer = new String[rawColumnCodec.getColumnsCount()]; // stored encoded value

        bytesSplitter = new BytesSplitter(kapConfig.getRawTableColumnCountMax(), kapConfig.getRawTableColumnLengthMax());
        initNullBytes();

        RawToGridTableMapping gridTableMapping = rawTableDesc.getRawToGridTableMapping();
        shardbyMapping = initIndexMapping(rawTableDesc.getShardbyColumns(), gridTableMapping);
        sortbyMapping = initIndexMapping(rawTableDesc.getShardbyColumns(), gridTableMapping);
        nonSortbyMapping = initIndexMapping(rawTableDesc.getNonSortbyColumns(), gridTableMapping);
    }

    /**
     * Create index mapping in source and rawtable
     * @param columns originColumns
     * @return <Index in source, Index in rawtable> pair
     */
    private Pair<Integer, Integer>[] initIndexMapping(Collection<TblColRef> columns, RawToGridTableMapping mapping) {
        Pair<Integer, Integer>[] indexMapping = new Pair[columns.size()];
        int index = 0;
        for (TblColRef col : columns) {
            indexMapping[index] = new Pair<>(intermediateTableDesc.getColumnIndex(col), mapping.getIndexOf(col));
            index++;
        }
        return indexMapping;
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

        outputKey = buildKey(bytesSplitter.getSplitBuffers());
        outputValue = buildValue(bytesSplitter.getSplitBuffers());
        context.write(outputKey, outputValue);
    }

    /**
     * Build output key part (shardId + sortby columns)
     * @param splitBuffers
     * @return
     */
    protected ByteArrayListWritable buildKey(SplittedBytes[] splitBuffers) {
        ByteArrayListWritable curKey = new ByteArrayListWritable();

        int shardNum = rawSegment.getShardNum() == 0 ? 2 : rawSegment.getShardNum();//by default 2 shards for raw table

        // write shard number
        ByteBuffer shardValue = getShardValue(splitBuffers);
        Short shardId = ShardingHash.getShard(shardValue.array(), 0, shardValue.position(), shardNum);
        curKey.add(BytesUtil.writeShort(shardId));

        // write sortby column
        for (byte[] sortby : encodeSortbyColumn(splitBuffers)) {
            curKey.add(sortby);
        }

        return curKey;
    }

    /**
     * Build output value part (non-sortby columns)
     * @param splitBuffers
     * @return
     */
    protected ByteArrayListWritable buildValue(SplittedBytes[] splitBuffers) {
        return new ByteArrayListWritable(encodeNonSortbyColumns(splitBuffers));
    }

    protected List<byte[]> encodeSortbyColumn(SplittedBytes[] splitBuffers) {
        return encodingColumns(splitBuffers, sortbyMapping);
    }

    protected List<byte[]> encodeNonSortbyColumns(SplittedBytes[] splitBuffers) {
        return encodingColumns(splitBuffers, nonSortbyMapping);
    }

    private List<byte[]> encodingColumns(SplittedBytes[] splitBuffers, Pair<Integer, Integer>[] columnMapping) {
        List<byte[]> result = Lists.newArrayList();
        for (int i = 0; i < columnMapping.length; i++) {
            int idInSource = columnMapping[i].getFirst();
            int idInRawTable = columnMapping[i].getSecond();
            if (isNull(splitBuffers[idInSource].value, 0, splitBuffers[idInSource].length)) {
                codecBuffer[idInRawTable] = null;
            } else {
                codecBuffer[idInRawTable] = Bytes.toString(splitBuffers[idInSource].value, 0, splitBuffers[idInSource].length);
            }
            ImmutableBitSet rawtableIndexBitmap = new ImmutableBitSet(idInRawTable);
            ByteBuffer buffer = rawColumnCodec.encode(RawValueIngester.buildObjectOf(codecBuffer, rawColumnCodec, rawtableIndexBitmap), rawtableIndexBitmap);
            result.add(Arrays.copyOfRange(buffer.array(), 0, buffer.position()));
        }

        return result;
    }

    protected ByteBuffer getShardValue(SplittedBytes[] splitBuffers) {
        for (int i = 0; i < shardbyMapping.length; i++) {
            int idInSource = shardbyMapping[i].getFirst();
            int idInRawTable = shardbyMapping[i].getSecond();
            if (isNull(splitBuffers[idInSource].value, 0, splitBuffers[idInSource].length)) {
                codecBuffer[idInRawTable] = null;
            } else {
                codecBuffer[idInRawTable] = Bytes.toString(splitBuffers[idInSource].value, 0, splitBuffers[idInSource].length);
            }
        }
        Object[] values = RawValueIngester.buildObjectOf(codecBuffer, rawColumnCodec, rawTableDesc.getRawToGridTableMapping().getShardbyKey());
        return rawColumnCodec.encode(values, rawTableDesc.getRawToGridTableMapping().getShardbyKey());
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
