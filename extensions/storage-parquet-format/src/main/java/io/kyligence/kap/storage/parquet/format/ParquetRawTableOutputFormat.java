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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.BufferedRawColumnCodec;
import io.kyligence.kap.cube.raw.RawTableColumnFamilyDesc;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;
import io.kyligence.kap.cube.raw.gridtable.RawTableGridTable;
import io.kyligence.kap.cube.raw.kv.RawTableConstants;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

public class ParquetRawTableOutputFormat extends FileOutputFormat<ByteArrayListWritable, ByteArrayListWritable> {
    @Override
    public RecordWriter<ByteArrayListWritable, ByteArrayListWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        return new ParquetRawTableFileWriter((FileOutputCommitter) this.getOutputCommitter(job), job,
                job.getOutputKeyClass(), job.getOutputValueClass());
    }

    public static class ParquetRawTableFileWriter
            extends ParquetOrderedFileWriter<ByteArrayListWritable, ByteArrayListWritable> {
        private static final Logger logger = LoggerFactory.getLogger(ParquetRawTableFileWriter.class);

        private short curShardId = (short) -1;
        private int curShardCounter = 0;

        private Configuration config;
        private KylinConfig kylinConfig;
        private RawTableInstance rawTableInstance;
        private RawTableDesc rawTableDesc;
        private BufferedRawColumnCodec rawColumnsCodec;
        private Path outputDir = null;
        private RawTableColumnFamilyDesc[] cfDescs;

        public ParquetRawTableFileWriter(FileOutputCommitter committer, TaskAttemptContext context, Class<?> keyClass,
                Class<?> valueClass) throws IOException, InterruptedException {
            this.config = context.getConfiguration();
            this.outputDir = committer.getWorkPath();

            logger.info("tmp output dir: {}", outputDir);

            kylinConfig = KylinConfig.loadKylinPropsAndMetadata();

            String rawName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
            logger.info("RawTableName is " + rawName + " and segmentID is " + segmentID);
            rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(rawName);
            rawTableDesc = rawTableInstance.getRawTableDesc();
            GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableDesc);
            rawColumnsCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());
            cfDescs = rawTableDesc.getRawTableMapping().getColumnFamily();

            // FIXME: ByteArrayListWritable involves array copy every time
            if (keyClass == ByteArrayListWritable.class && valueClass == ByteArrayListWritable.class) {
                logger.info("KV class is ByteArrayListWritable");
            } else {
                throw new InvalidParameterException("ParquetRecordWriter only supports ByteArrayListWritable type now");
            }
        }

        @Override
        protected void freshWriter(ByteArrayListWritable key, ByteArrayListWritable value)
                throws IOException, InterruptedException {
            short shardId = BytesUtil.readShort(key.get().get(key.get().size() - 1));

            if (shardId != curShardId) {
                cleanWriter();
                curShardId = shardId;
                curShardCounter = 0;
                logger.info("meet a new raw table shard: shard {}", curShardId);

            }

            curShardCounter++;

            if (writer == null) {
                writer = newWriter();
            }
        }

        @Override
        protected void cleanWriter() throws IOException {
            logger.info("Finish written {} lines for shard {}", curShardCounter, curShardId);
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        @Override
        protected ParquetRawWriter newWriter() throws IOException, InterruptedException {
            ParquetRawWriter rawWriter;
            List<Type> types = new ArrayList<Type>();

            RawTableColumnFamilyDesc[] cfDescs = rawTableDesc.getRawTableMapping().getColumnFamily();
            for (RawTableColumnFamilyDesc cf : cfDescs) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
                        cf.getName()));
            }

            MessageType schema = new MessageType(rawTableDesc.getName(), types);
            rawWriter = new ParquetRawWriter.Builder()
                    .setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage())//
                    .setPagesPerGroup(KapConfig.getInstanceFromEnv().getParquetPagesPerGroup())//
                    .setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression()).setConf(config)
                    .setType(schema).setPath(getOutputPath())
                    .setThresholdMemory(KapConfig.getInstanceFromEnv().getParquetRawWriterThresholdMB()).build();
            return rawWriter;
        }

        @Override
        protected void writeData(ByteArrayListWritable key, ByteArrayListWritable value) throws IOException {

            // Step 1: transform text object to byte array. 
            List<byte[]> sortby = key.get().subList(0, key.get().size() - 1);
            List<byte[]> nonSortby = value.get();
            int valueBytesLength = 0;
            for (byte[] objInSortby : sortby) {
                valueBytesLength += objInSortby.length;
            }
            for (byte[] objInNonSortby : nonSortby) {
                valueBytesLength += objInNonSortby.length;
            }
            byte[] valueBytes = new byte[valueBytesLength];
            int idx = 0;
            for (byte[] objInSortby : sortby) {
                System.arraycopy(objInSortby, 0, valueBytes, idx, objInSortby.length);
                idx += objInSortby.length;
            }
            for (byte[] objInNonSortby : nonSortby) {
                System.arraycopy(objInNonSortby, 0, valueBytes, idx, objInNonSortby.length);
                idx += objInNonSortby.length;
            }

            // Step 2: calculate value offsets in result byte array to which measures will be copied.
            int[] valueLength = rawColumnsCodec.peekLengths(ByteBuffer.wrap(valueBytes),
                    new ImmutableBitSet(0, sortby.size() + nonSortby.size()));
            int[] valueOffsets = new int[valueLength.length];
            for (int i = 0, valueOffset = 0; i < valueOffsets.length; i++) {
                valueOffsets[i] = valueOffset;
                valueOffset += valueLength[i];
            }

            // Step 3: copy array bytes as column family order. 
            byte[] cfValueBytes = new byte[valueBytes.length];
            int[] cfValueLength = new int[cfDescs.length];
            int cfIndex = 0, cfValueOffset = 0;
            for (RawTableColumnFamilyDesc cfDesc : cfDescs) {
                int cfLength = 0;
                int[] columnIndexes = cfDesc.getColumnIndex();
                for (int columnIndex : columnIndexes) {
                    System.arraycopy(valueBytes, valueOffsets[columnIndex], cfValueBytes, cfValueOffset,
                            valueLength[columnIndex]);
                    cfValueOffset += valueLength[columnIndex];
                    cfLength += valueLength[columnIndex];
                }
                cfValueLength[cfIndex] = cfLength;
                cfIndex++;
            }

            writer.writeRow(cfValueBytes, cfValueLength);
        }

        @Override
        protected Path getOutputPath() {
            Path path = new Path(outputDir, new StringBuffer().append(RawTableConstants.RawTableDir).append("/")
                    .append(curShardId).append(".parquet").toString());
            return path;
        }
    }
}
