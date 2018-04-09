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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.storage.parquet.format.file.ParquetSpliceWriter;

/**
 * cube splice build output format
 */
public class ParquetCubeSpliceOutputFormat extends FileOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ParquetCubeSpliceWriter((FileOutputCommitter) this.getOutputCommitter(job), job,
                job.getOutputKeyClass(), job.getOutputValueClass());
    }

    public static class ParquetCubeSpliceWriter extends RecordWriter<Text, Text> {
        private static final Logger logger = LoggerFactory.getLogger(ParquetCubeSpliceWriter.class);

        private long curCuboidId = -1;
        private short curShardId = -1;

        private Configuration config;
        private KylinConfig kylinConfig;
        private MeasureCodec measureCodec;
        private CubeSegment cubeSegment;
        private Path outputDir = null;
        private HBaseColumnFamilyDesc[] cfDescs;

        private ParquetSpliceWriter writer = null;

        public ParquetCubeSpliceWriter(FileOutputCommitter committer, TaskAttemptContext context, Class<?> keyClass,
                Class<?> valueClass) throws IOException, InterruptedException {
            this.config = context.getConfiguration();
            this.outputDir = committer.getWorkPath();

            logger.info("tmp output dir: {}", outputDir);
            logger.info("final output dir: {}", committer.getCommittedTaskPath(context));

            String jobType = context.getConfiguration().get(BatchConstants.CFG_MR_SPARK_JOB);

            if ("SparkOnYarn".equals(jobType)) {
                String metaUrl = context.getConfiguration().get(BatchConstants.CFG_SPARK_META_URL);
                kylinConfig = KylinConfig.loadKylinConfigFromHdfsIfNeeded(metaUrl);
            } else if ("SparkLocal".equalsIgnoreCase(jobType)) {
                String metaUrl = context.getConfiguration().get(BatchConstants.CFG_SPARK_META_URL);
                kylinConfig = KylinConfig.createInstanceFromUri(metaUrl);
                KylinConfig.setKylinConfigThreadLocal(kylinConfig);
            } else
                kylinConfig = KylinConfig.loadKylinPropsAndMetadata();

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);

            this.cubeSegment = CubeManager.getInstance(kylinConfig).getCube(cubeName).getSegmentById(segmentID);
            Preconditions.checkState(cubeSegment.isEnableSharding(),
                    "Cube segment sharding not enabled " + cubeSegment.getName());

            this.measureCodec = new MeasureCodec(cubeSegment.getCubeDesc().getMeasures());
            this.cfDescs = cubeSegment.getCubeDesc().getHbaseMapping().getColumnFamily();
            if (keyClass == Text.class && valueClass == Text.class) {
                logger.info("KV class is Text");
            } else {
                throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
            }
            this.writer = newWriter();
        }

        // This constructor is only used for testing. 
        protected ParquetCubeSpliceWriter(Configuration conf, Path path, CubeSegment cubeSegment)
                throws IOException, InterruptedException {

            this.config = conf;
            this.outputDir = path;
            this.cubeSegment = cubeSegment;

            this.measureCodec = new MeasureCodec(cubeSegment.getCubeDesc().getMeasures());
            this.cfDescs = cubeSegment.getCubeDesc().getHbaseMapping().getColumnFamily();

            this.writer = newWriter();
        }

        protected void freshWriter(Text key) throws InterruptedException, IOException {
            byte[] keyBytes = key.getBytes();
            long cuboidId = Bytes.toLong(keyBytes, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
            short shardId = Bytes.toShort(keyBytes, 0, RowConstants.ROWKEY_SHARDID_LEN);

            if (shardId != curShardId || cuboidId != curCuboidId) {
                curShardId = shardId;
                curCuboidId = cuboidId;
                writer.endDiv();
                writer.startDiv(buildDiv(curCuboidId, curShardId));
            }
        }

        public static String buildDiv(long cuboidId, short shardId) {
            return cuboidId + "-" + shardId;
        }

        public static long getCuboididFromDiv(String div) {
            return Long.valueOf(div.split("-")[0]);
        }

        public static short getShardidFromDiv(String div) {
            return Short.valueOf(div.split("-")[1]);
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {

            freshWriter(key);

            // Log for large size row
            if (value.getBytes().length > MemoryBudgetController.ONE_MB * 50) {
                logger.info("Writing row with size " + (value.getBytes().length / MemoryBudgetController.ONE_MB)
                        + "MB to cuboid with ID " + curCuboidId
                        + ", please increase the setting memory of \"mapreduce.reduce.memory.mb\" and \"mapreduce.reduce.java.opts\" in \"kylin_job_conf.xml\" if out of memory error occurs.");
            }

            // Step 1: transform text object to byte array. 
            byte[] valueBytes = Arrays.copyOf(value.getBytes(), value.getLength()); //on purpose, because Parquet writer will cache
            byte[] keyBody = Arrays.copyOfRange(key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN,
                    key.getLength());
            int[] valueLength = measureCodec.getPeekLength(ByteBuffer.wrap(valueBytes));

            // Step 2: calculate value offsets in result byte array to which measures will be copied.
            int[] valueOffsets = new int[valueLength.length];
            for (int i = 0, valueOffset = 0; i < valueOffsets.length; i++) {
                valueOffsets[i] = valueOffset;
                valueOffset += valueLength[i];
            }

            // Step 3: copy array bytes as column family order. 
            byte[] cfValueBytes = new byte[valueBytes.length];
            int[] cfValueLength = new int[cfDescs.length];
            int cfIndex = 0, cfValueOffset = 0;
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                int cfLength = 0;
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    int[] measureIndexes = colDesc.getMeasureIndex();
                    for (int measureIndex : measureIndexes) {
                        System.arraycopy(valueBytes, valueOffsets[measureIndex], cfValueBytes, cfValueOffset,
                                valueLength[measureIndex]);
                        cfValueOffset += valueLength[measureIndex];
                        cfLength += valueLength[measureIndex];
                    }
                }
                cfValueLength[cfIndex] = cfLength;
                cfIndex++;
            }

            try {
                writer.writeRow(keyBody, 0, keyBody.length, cfValueBytes, cfValueLength);
            } catch (Exception e) {
                logger.error("", e);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // close div inside
            writer.close();
        }

        protected ParquetSpliceWriter newWriter() throws IOException, InterruptedException {
            List<Type> types = new ArrayList<Type>();
            // row key
            types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "Row Key"));

            // column families
            HBaseColumnFamilyDesc[] cfDescs = cubeSegment.getCubeDesc().getHbaseMapping().getColumnFamily();
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
                            colDesc.getColumnFamilyName()));
                }
            }

            MessageType schema = new MessageType(cubeSegment.getName(), types);
            ParquetSpliceWriter writer = new ParquetSpliceWriter.Builder()
                    .setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage())//
                    .setPagesPerGroup(KapConfig.getInstanceFromEnv().getParquetPagesPerGroup())//
                    .setThresholdMemory(KapConfig.getInstanceFromEnv().getParquetRawWriterThresholdMB())//
                    .setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression())//
                    .setConf(config).setType(schema).setPath(getOutputPath()).build();
            return writer;
        }

        // Generate 10-length random string file name
        private Path getOutputPath() {
            Path path = new Path(outputDir,
                    new StringBuffer().append(RandomStringUtils.randomAlphabetic(10)).append(".parquet").toString());
            return path;
        }
    }
}
