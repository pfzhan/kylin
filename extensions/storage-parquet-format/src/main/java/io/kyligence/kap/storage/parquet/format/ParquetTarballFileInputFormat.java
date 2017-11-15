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

import static io.kyligence.kap.storage.parquet.format.ParquetFormatConstants.KYLIN_DEFAULT_GT_MAX_LENGTH;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.filter.BinaryFilter;
import io.kyligence.kap.storage.parquet.format.filter.BinaryFilterSerializer;
import io.kyligence.kap.storage.parquet.format.filter.MassInValueProviderFactoryImpl;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;

/**
 * spark rdd input and merge job
 */
public class ParquetTarballFileInputFormat extends FileInputFormat<Text, Text> {

    public org.apache.hadoop.mapreduce.RecordReader<Text, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetTarballFileReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class ParquetTarballFileReader extends RecordReader<Text, Text> {

        public enum ReadStrategy {
            KV, COMPACT
        }

        public static final Logger logger = LoggerFactory.getLogger(ParquetTarballFileReader.class);
        public static ThreadLocal<GTScanRequest> gtScanRequestThreadLocal = new ThreadLocal<>();

        protected Configuration conf;

        private ParquetBundleReader reader = null;
        private ParquetPageIndexTable indexTable = null;
        private BinaryFilter binaryFilter = null;
        private Text key = null; //key will be fixed length,
        private Text val = null; //reusing the val bytes, the returned bytes might contain useless tail, but user will use it as bytebuffer, so it's okay

        private ReadStrategy readStrategy;
        private long cuboidId;
        private short shardId;
        // profile
        private long totalScanCnt = 0;
        private long totalSkipCnt = 0;
        private ByteArray byteArray = new ByteArray();

        long profileStartTime = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            Path shardPath = fileSplit.getPath();

            logger.info("tarball file: {}", shardPath);

            long startTime = System.currentTimeMillis();
            String kylinPropsStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, "");
            if (kylinPropsStr.isEmpty()) {
                logger.warn("Creating an empty KylinConfig");
            }
            logger.info("Creating KylinConfig from conf");
            KylinConfig.setKylinConfigInEnvIfMissing(kylinPropsStr);

            FileSystem fileSystem = shardPath.getFileSystem(conf);
            FSDataInputStream inputStream = fileSystem.open(shardPath);
            long fileOffset = inputStream.readLong();//read the offset
            int indexOffset = ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE;
            indexTable = new ParquetPageIndexTable(fileSystem, shardPath, inputStream, indexOffset);

            String binaryFilterStr = conf.get(ParquetFormatConstants.KYLIN_BINARY_FILTER);
            if (binaryFilterStr != null && binaryFilterStr.trim().length() != 0) {
                binaryFilter = BinaryFilterSerializer.deserialize(ByteBuffer.wrap(binaryFilterStr.getBytes("ISO-8859-1")));
            }
            logger.info("Binary Filter: {}", binaryFilter);

            String scanReqStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES);
            ImmutableRoaringBitmap pageBitmap = null;
            if (scanReqStr != null) {
                final GTScanRequest gtScanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(scanReqStr.getBytes("ISO-8859-1")));
                gtScanRequestThreadLocal.set(gtScanRequest);//for later use convenience

                MassInTupleFilter.VALUE_PROVIDER_FACTORY = new MassInValueProviderFactoryImpl(new MassInValueProviderFactoryImpl.DimEncAware() {
                    @Override
                    public DimensionEncoding getDimEnc(TblColRef col) {
                        return gtScanRequest.getInfo().getCodeSystem().getDimEnc(col.getColumnDesc().getZeroBasedIndex());
                    }
                });

                if (Boolean.valueOf(conf.get(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX))) {
                    TupleFilter filter = gtScanRequest.getFilterPushDown();

                    logger.info("Starting to lookup inverted index");
                    pageBitmap = indexTable.lookup(filter);
                    logger.info("Inverted Index bitmap: {}", pageBitmap);
                    logger.info("read index takes: {} ms", (System.currentTimeMillis() - startTime));
                } else {
                    logger.info("Told not to use II, read all pages");
                }
            } else {
                logger.info("KYLIN_SCAN_REQUEST_BYTES not set, read all pages");
            }

            ImmutableRoaringBitmap columnBitmap = RoaringBitmaps.readFromString(conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS));
            if (columnBitmap != null) {
                logger.info("All columns read by parquet: " + StringUtils.join(columnBitmap, ","));
            } else {
                logger.info("All columns read by parquet is not set");
            }

            //for readStrategy
            readStrategy = ReadStrategy.valueOf(conf.get(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY));
            cuboidId = Long.valueOf(shardPath.getParent().getName());
            shardId = Short.valueOf(shardPath.getName().substring(0, shardPath.getName().indexOf(".parquettar")));

            logger.info("Read Strategy is {} Cuboid id is {} and shard id is {}", readStrategy.toString(), cuboidId, shardId);

            String gtMaxLengthStr = conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH);
            int gtMaxLength = gtMaxLengthStr == null ? KYLIN_DEFAULT_GT_MAX_LENGTH : Integer.valueOf(gtMaxLengthStr);

            val = new Text();
            val.set(new byte[gtMaxLength]);

            if (pageBitmap != null && pageBitmap.isEmpty()) {
                reader = null;
            } else {
                // init with first shard file
                reader = new ParquetBundleReader.Builder().setFileOffset(fileOffset).setConf(conf).setPath(shardPath).setPageBitset(pageBitmap).setColumnsBitmap(columnBitmap).build();
            }

            // finish initialization
            profileStartTime = System.currentTimeMillis();
        }

        // test only
        void setReader(ParquetBundleReader reader) {
            this.reader = reader;
        }

        // test only
        void setBinaryFilter(BinaryFilter filter) {
            this.binaryFilter = filter;
        }

        // test only
        void setReadStrategy(ReadStrategy strategy) {
            this.readStrategy = strategy;
        }

        // test only
        void setDefaultValue(byte[] value) {
            if (val == null) {
                val = new Text();
            }
            this.val.set(value);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // Read nothing if the bitmap is empty
            if (reader == null) {
                return false;
            }

            List<Object> data = null;
            while (true) {
                data = reader.read();
                if (data == null) {
                    return false;
                }
                totalScanCnt++;
                byte[] firstCol = ((Binary) data.get(0)).getBytes();
                byteArray.reset(firstCol, 0, firstCol.length);
                if (binaryFilter != null && !binaryFilter.isMatch(byteArray)) {
                    totalSkipCnt++;
                    continue;
                }
                break;
            }

            if (readStrategy == ReadStrategy.KV) {
                // key
                byte[] keyBytes = ((Binary) data.get(0)).getBytes();
                if (key == null) {
                    key = new Text();
                    byte[] temp = new byte[keyBytes.length + RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN];//make sure length
                    key.set(temp);
                    Preconditions.checkState(Shorts.BYTES == RowConstants.ROWKEY_SHARDID_LEN);
                    System.arraycopy(Bytes.toBytes(shardId), 0, key.getBytes(), 0, Shorts.BYTES);
                    Preconditions.checkState(Longs.BYTES == RowConstants.ROWKEY_CUBOIDID_LEN);
                    System.arraycopy(Bytes.toBytes(cuboidId), 0, key.getBytes(), Shorts.BYTES, Longs.BYTES);
                }
                System.arraycopy(keyBytes, 0, key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, keyBytes.length);

                //value
                setVal(data, 1);
            } else if (readStrategy == ReadStrategy.COMPACT) {
                if (key == null) {
                    key = new Text();
                }
                setVal(data, 0);
            } else {
                throw new RuntimeException("unknown read strategy: " + readStrategy);
            }
            return true;
        }

        private void setVal(List<Object> data, int start) {
            int retry = 0;
            while (true) {
                try {
                    int offset = 0;
                    for (int i = start; i < data.size(); ++i) {
                        byte[] src = ((Binary) data.get(i)).getBytes();
                        System.arraycopy(src, 0, val.getBytes(), offset, src.length);
                        offset += src.length;
                    }
                    break;
                } catch (ArrayIndexOutOfBoundsException e) {
                    if (++retry > 10) {
                        throw new IllegalStateException("Measures taking too much space! ");
                    }
                    byte[] temp = new byte[val.getBytes().length * 2];
                    val.set(temp);
                    logger.info("val buffer size adjusted to: " + val.getBytes().length);
                }
            }
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
            logger.info("read file takes {} ms", System.currentTimeMillis() - profileStartTime);
            logger.info("total scan {} rows, skip {} rows", totalScanCnt, totalSkipCnt);
            if (reader != null) {
                reader.close();
            }

            if (indexTable != null) {
                indexTable.close();
            }
        }
    }
}
