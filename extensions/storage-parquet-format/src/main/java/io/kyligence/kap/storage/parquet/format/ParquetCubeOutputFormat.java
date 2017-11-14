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
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

/**
 * cube build output format
 */
public class ParquetCubeOutputFormat extends FileOutputFormat<Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(ParquetCubeOutputFormat.class);

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ParquetCubeWriter((FileOutputCommitter) this.getOutputCommitter(job), job, job.getOutputKeyClass(),
                job.getOutputValueClass());
    }

    public static class ParquetCubeWriter extends ParquetOrderedFileWriter<Text, Text> {
        private static final Logger logger = LoggerFactory.getLogger(ParquetCubeWriter.class);

        private long curCuboidId = -1;
        private short curShardId = -1;
        private int curShardCounter = 0;

        private Configuration config;
        private KylinConfig kylinConfig;
        private MeasureCodec measureCodec;
        private CubeInstance cubeInstance;
        private CubeSegment cubeSegment;
        private Path outputDir = null;

        public ParquetCubeWriter(FileOutputCommitter committer, TaskAttemptContext context, Class<?> keyClass,
                Class<?> valueClass) throws IOException, InterruptedException {
            this.config = context.getConfiguration();
            this.outputDir = committer.getWorkPath();

            logger.info("tmp output dir: {}", outputDir);
            logger.info("final output dir: {}", committer.getCommittedTaskPath(context));

            String jobType = context.getConfiguration().get(BatchConstants.CFG_MR_SPARK_JOB);

            if ("spark".equals(jobType)) {
                String metaUrl = context.getConfiguration().get(BatchConstants.CFG_SPARK_META_URL);
                kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaUrl);
            } else
                kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);
            cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            cubeSegment = cubeInstance.getSegmentById(segmentID);
            Preconditions.checkState(cubeSegment.isEnableSharding(),
                    "Cube segment sharding not enabled " + cubeSegment.getName());

            measureCodec = new MeasureCodec(cubeSegment.getCubeDesc().getMeasures());

            if (keyClass == Text.class && valueClass == Text.class) {
                logger.info("KV class is Text");
            } else {
                throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
            }
        }

        @Override
        protected void freshWriter(Text key, Text value) throws InterruptedException, IOException {
            byte[] keyBytes = key.getBytes();
            long cuboidId = Bytes.toLong(keyBytes, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
            short shardId = Bytes.toShort(keyBytes, 0, RowConstants.ROWKEY_SHARDID_LEN);

            if (shardId != curShardId || cuboidId != curCuboidId) {
                cleanWriter();
                curShardId = shardId;
                curCuboidId = cuboidId;

                curShardCounter = 0;
                logger.info("meet a new shard: cuboid {} shard {}", curCuboidId, curShardId);

            }

            curShardCounter++;

            if (writer == null) {
                writer = newWriter();
            }
        }

        @Override
        protected void cleanWriter() throws IOException {
            logger.info("Finish written {} lines for cuboid {} shard {}", curShardCounter, curCuboidId, curShardId);
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        @Override
        protected ParquetRawWriter newWriter() throws IOException, InterruptedException {
            ParquetRawWriter rawWriter = null;
            List<Type> types = new ArrayList<Type>();

            types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "Row Key"));

            // measures
            for (MeasureDesc measure : cubeSegment.getCubeDesc().getMeasures()) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
                        measure.getName()));
            }

            MessageType schema = new MessageType(cubeSegment.getName(), types);
            rawWriter = new ParquetRawWriter.Builder()
                    .setRowsPerPage(KapConfig.wrap(cubeSegment.getConfig()).getParquetRowsPerPage())//
                    .setPagesPerGroup(KapConfig.wrap(cubeSegment.getConfig()).getParquetPagesPerGroup())
                    .setCodecName(KapConfig.wrap(cubeSegment.getConfig()).getParquetPageCompression()).setConf(config)
                    .setType(schema).setPath(getOutputPath())
                    .setThresholdMemory(KapConfig.wrap(cubeSegment.getConfig()).getParquetRawWriterThresholdMB())
                    .build();
            return rawWriter;
        }

        @Override
        protected void writeData(Text key, Text value) throws IOException {
            byte[] valueBytes = value.getBytes().clone(); //on purpose, because parquet writer will cache
            byte[] keyBody = Arrays.copyOfRange(key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN,
                    key.getLength());
            int[] valueLength = measureCodec.getPeekLength(ByteBuffer.wrap(valueBytes));
            writer.writeRow(keyBody, 0, keyBody.length, valueBytes, valueLength);
        }

        @Override
        protected Path getOutputPath() {
            Path path = new Path(outputDir, new StringBuffer().append(curCuboidId).append("/").append(curShardId)
                    .append(".parquet").toString());
            return path;
        }
    }
}
