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

package io.kyligence.kap.storage.parquet.format;

import static io.kyligence.kap.storage.parquet.format.ParquetCubeSpliceOutputFormat.ParquetCubeSpliceWriter.getCuboididFromDiv;
import static io.kyligence.kap.storage.parquet.format.ParquetCubeSpliceOutputFormat.ParquetCubeSpliceWriter.getShardidFromDiv;
import static io.kyligence.kap.storage.parquet.format.ParquetFormatConstants.KYLIN_DEFAULT_GT_MAX_LENGTH;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetSpliceReader;
import io.kyligence.kap.storage.parquet.format.filter.BinaryFilter;
import io.kyligence.kap.storage.parquet.format.filter.BinaryFilterSerializer;
import io.kyligence.kap.storage.parquet.format.filter.MassInValueProviderFactoryImpl;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexReader;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexSpliceReader;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;

/**
 * cube merge input
 */
public class ParquetSpliceTarballFileMergeInputFormat extends FileInputFormat<Text, Text> {

    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new ParquetTarballFileReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class ParquetTarballFileReader extends RecordReader<Text, Text> {

        public static final Logger logger = LoggerFactory.getLogger(ParquetTarballFileReader.class);
        public static ThreadLocal<GTScanRequest> gtScanRequestThreadLocal = new ThreadLocal<>();

        protected Configuration conf;

        private KylinConfig kylinConfig;
        private CubeInstance cubeInstance;
        private CubeSegment cubeSegment;

        private HBaseColumnFamilyDesc[] cfDescs;
        private MeasureCodec measureCodec;

        private BinaryFilter binaryFilter = null;
        private ParquetBundleReader reader = null;
        private ParquetPageIndexTable indexTable = null;
        private Text key = null; //key will be fixed length,
        private Text val = null; //reusing the val bytes, the returned bytes might contain useless tail, but user will use it as bytebuffer, so it's okay

        private ImmutableRoaringBitmap columnBitmap;
        private Map<Integer, Short> page2ShardMap;
        private Map<Integer, Long> page2CuboidMap;
        private long cuboidId = -1;
        private long curPageIndex = -1;
        private int curKeyByteLength = -1;

        long profileStartTime = 0;

        private long totalScanCnt = 0;
        private long totalSkipCnt = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            Path path = ((FileSplit) split).getPath();
            this.conf = context.getConfiguration();

            logger.info("tarball file: {}", path);

            this.kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);

            this.cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            this.cubeSegment = cubeInstance.getSegmentById(segmentID);
            this.cfDescs = cubeSegment.getCubeDesc().getHbaseMapping().getColumnFamily();
            List<MeasureDesc> cfMeasures = Lists.newArrayList();
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    MeasureDesc[] measures = colDesc.getMeasures();
                    cfMeasures.addAll(new ArrayList<MeasureDesc>(Arrays.asList(measures)));
                }
            }
            this.measureCodec = new MeasureCodec(cfMeasures);

            long startTime = System.currentTimeMillis();

            // Column bitmap
            columnBitmap = RoaringBitmaps
                    .readFromString(conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS));
            if (columnBitmap != null) {
                logger.info("All columns read by parquet: " + StringUtils.join(columnBitmap, ","));
            } else {
                logger.info("All columns read by parquet is not set");
            }

            String binaryFilterStr = conf.get(ParquetFormatConstants.KYLIN_BINARY_FILTER);
            if (binaryFilterStr != null && !binaryFilterStr.trim().isEmpty()) {
                binaryFilter = BinaryFilterSerializer
                        .deserialize(ByteBuffer.wrap(binaryFilterStr.getBytes("ISO-8859-1")));
            }
            logger.info("Binary Filter: {}", binaryFilter);

            // Index length (parquet file start offset)
            FileSystem fileSystem = HadoopUtil.getFileSystem(path, conf);
            FSDataInputStream inputStream = fileSystem.open(path);
            long indexLength = inputStream.readLong();

            // Required divs
            if (conf.get(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS) != null) {
                cuboidId = Long.valueOf(conf.get(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS));
            }

            logger.info("Build page to shardId map");
            ParquetSpliceReader spliceReader = new ParquetSpliceReader.Builder().setConf(conf).setPath(path)
                    .setColumnsBitmap(columnBitmap).setFileOffset(indexLength).build();
            page2ShardMap = Maps.newHashMap();
            page2CuboidMap = Maps.newHashMap();
            for (String d : spliceReader.getDivs()) {
                long cuboidId = getCuboididFromDiv(d);
                short shardId = getShardidFromDiv(d);
                Pair<Integer, Integer> range = spliceReader.getDivPageRange(d);
                for (int page = range.getLeft(); page < range.getRight(); page++) {
                    page2CuboidMap.put(page, cuboidId);
                    page2ShardMap.put(page, shardId);
                }
            }

            // Read page index if necessary, this block is only accessed in query
            String scanReqStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES);
            ImmutableRoaringBitmap pageBitmap = null;
            ParquetPageIndexSpliceReader pageIndexSpliceReader;
            try {
                pageIndexSpliceReader = new ParquetPageIndexSpliceReader(inputStream, indexLength,
                        ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (scanReqStr != null) {
                final GTScanRequest gtScanRequest = GTScanRequest.serializer
                        .deserialize(ByteBuffer.wrap(scanReqStr.getBytes("ISO-8859-1")));
                gtScanRequestThreadLocal.set(gtScanRequest);//for later use convenience
                MassInTupleFilter.VALUE_PROVIDER_FACTORY = new MassInValueProviderFactoryImpl(
                        new MassInValueProviderFactoryImpl.DimEncAware() {
                            @Override
                            public DimensionEncoding getDimEnc(TblColRef col) {
                                return gtScanRequest.getInfo().getCodeSystem()
                                        .getDimEnc(col.getColumnDesc().getZeroBasedIndex());
                            }
                        });

                TupleFilter filter = gtScanRequest.getFilterPushDown();
                if (Boolean.valueOf(conf.get(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX))
                        && !filterIsNull(filter)) {
                    MutableRoaringBitmap mutableRoaringBitmap = null;
                    for (ParquetPageIndexReader divIndexReader : pageIndexSpliceReader
                            .getIndexReaderByCuboid(cuboidId)) {
                        indexTable = new ParquetPageIndexTable(fileSystem, path, divIndexReader);
                        if (mutableRoaringBitmap == null) {
                            mutableRoaringBitmap = new MutableRoaringBitmap(
                                    indexTable.lookup(filter).toRoaringBitmap());
                        } else {
                            mutableRoaringBitmap.or(indexTable.lookup(filter));
                        }
                        indexTable.closeWithoutStream();
                    }
                    pageBitmap = mutableRoaringBitmap.toImmutableRoaringBitmap();

                    logger.info("Inverted Index bitmap: {}", pageBitmap);
                    logger.info("read index takes: {} ms", (System.currentTimeMillis() - startTime));
                }
            }

            // query whole cuboid
            if (pageBitmap == null && cuboidId >= 0) {
                pageBitmap = pageIndexSpliceReader.getFullBitmap(cuboidId);
            }

            reader = new ParquetBundleReader.Builder().setConf(conf).setPath(path).setColumnsBitmap(columnBitmap)
                    .setPageBitset(pageBitmap).setFileOffset(indexLength).build();

            // init val
            String gtMaxLengthStr = conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH);
            int gtMaxLength = gtMaxLengthStr == null ? KYLIN_DEFAULT_GT_MAX_LENGTH : Integer.valueOf(gtMaxLengthStr);
            val = new Text();
            val.set(new byte[gtMaxLength]);

            // finish initialization
            profileStartTime = System.currentTimeMillis();
        }

        private boolean filterIsNull(TupleFilter filter) {
            if (filter == null) {
                return true;
            }

            for (TupleFilter child : filter.getChildren()) {
                if (child != null) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // Read nothing if the bitmap is empty
            if (reader == null) {
                return false;
            }

            List<Object> data = null;
            while (data == null) {
                data = reader.read();
                if (data == null) {
                    return false;
                }
                totalScanCnt++;
                if (binaryFilter != null && !binaryFilter.isMatch(((Binary) data.get(0)).getBytes())) {
                    totalSkipCnt++;
                    data = null;
                }
            }

            // key
            byte[] keyBytes = ((Binary) data.get(0)).getBytes();
            if (key == null) {
                key = new Text();
            }
            if (curPageIndex != reader.getPageIndex()) {
                curPageIndex = reader.getPageIndex();
                if (keyBytes.length != curKeyByteLength) {
                    byte[] temp = new byte[keyBytes.length + RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN];//make sure length
                    key.set(temp);
                }
                System.arraycopy(Bytes.toBytes(page2ShardMap.get(reader.getPageIndex())), 0, key.getBytes(), 0,
                        Shorts.BYTES);
                System.arraycopy(Bytes.toBytes(page2CuboidMap.get(reader.getPageIndex())), 0, key.getBytes(),
                        Shorts.BYTES, Longs.BYTES);
            }
            System.arraycopy(keyBytes, 0, key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, keyBytes.length);

            //value
            setVal(data);

            return true;
        }

        private void setVal(List<Object> data) {

            // Step 1: transform data object list to byte array. 
            int cfValueBytesLength = 0;
            for (int i = 1; i < data.size(); ++i) {
                cfValueBytesLength += ((Binary) data.get(i)).getBytes().length;
            }
            byte[] cfValueBytes = new byte[cfValueBytesLength];
            for (int i = 1, cfIndex = 0; i < data.size(); ++i) {
                byte[] src = ((Binary) data.get(i)).getBytes();
                System.arraycopy(src, 0, cfValueBytes, cfIndex, src.length);
                cfIndex += src.length;
            }

            // Step 2: calculate byte array length for measures as the order they were defined.  
            int[] valueLength = measureCodec.getPeekLength(ByteBuffer.wrap(cfValueBytes));
            int[] valueLengthInMeasureOrder = new int[valueLength.length];
            int idx = 0;
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    int[] measureIndexes = colDesc.getMeasureIndex();
                    for (int measureIndex : measureIndexes) {
                        valueLengthInMeasureOrder[measureIndex] = valueLength[idx];
                        idx++;
                    }
                }
            }

            // Step 3: calculate value offsets in result byte array to which measures will be copied. 
            int[] valueOffsets = new int[valueLength.length];
            for (int i = 0, valueOffset = 0; i < valueOffsets.length; i++) {
                valueOffsets[i] = valueOffset;
                valueOffset += valueLengthInMeasureOrder[i];
            }

            // Step 4: copy array bytes as measure order. 
            byte[] valueBytes = new byte[cfValueBytes.length];
            int cfValueOffset = 0;
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    int[] measureIndexes = colDesc.getMeasureIndex();
                    for (int measureIndex : measureIndexes) {
                        System.arraycopy(cfValueBytes, cfValueOffset, valueBytes, valueOffsets[measureIndex],
                                valueLengthInMeasureOrder[measureIndex]);
                        cfValueOffset += valueLengthInMeasureOrder[measureIndex];
                    }
                }
            }

            val.set(valueBytes);
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return val;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            logger.info("total scan {} rows, skip {} rows", totalScanCnt, totalSkipCnt);
            logger.info("read file takes {} ms", System.currentTimeMillis() - profileStartTime);
            if (reader != null) {
                reader.close();
            }

            if (indexTable != null) {
                indexTable.close();
            }
        }
    }
}
