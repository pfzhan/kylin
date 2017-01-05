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

import io.kyligence.kap.storage.parquet.format.file.ParquetSpliceWriter;

/**
 * cube splice build output format
 */
public class ParquetCubeSpliceOutputFormat extends FileOutputFormat<Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(ParquetCubeSpliceOutputFormat.class);

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ParquetCubeSpliceWriter((FileOutputCommitter) this.getOutputCommitter(job), job, job.getOutputKeyClass(), job.getOutputValueClass());
    }

    public static class ParquetCubeSpliceWriter extends RecordWriter<Text, Text> {
        private static final Logger logger = LoggerFactory.getLogger(ParquetCubeSpliceWriter.class);

        private long curCuboidId = -1;
        private short curShardId = -1;

        private Configuration config;
        private KylinConfig kylinConfig;
        private MeasureCodec measureCodec;
        private CubeInstance cubeInstance;
        private CubeSegment cubeSegment;
        private Path outputDir = null;

        private ParquetSpliceWriter writer = null;

        public ParquetCubeSpliceWriter(FileOutputCommitter committer, TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException, InterruptedException {
            this.config = context.getConfiguration();
            this.outputDir = committer.getTaskAttemptPath(context);

            logger.info("tmp output dir: {}", outputDir);
            logger.info("final output dir: {}", committer.getCommittedTaskPath(context));

            kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);
            cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            cubeSegment = cubeInstance.getSegmentById(segmentID);
            Preconditions.checkState(cubeSegment.isEnableSharding(), "Cube segment sharding not enabled " + cubeSegment.getName());

            measureCodec = new MeasureCodec(cubeSegment.getCubeDesc().getMeasures());

            if (keyClass == Text.class && valueClass == Text.class) {
                logger.info("KV class is Text");
            } else {
                throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
            }
            writer = newWriter();
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

        public static long getCuboididFromDivgetCuboididFromDiv(String div) {
            return Long.valueOf(div.split("-")[0]);
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            freshWriter(key);
            byte[] valueBytes = value.getBytes().clone(); //on purpose, because parquet writer will cache
            byte[] keyBody = Arrays.copyOfRange(key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, key.getLength());
            int[] valueLength = measureCodec.getPeekLength(ByteBuffer.wrap(valueBytes));
            try {
                writer.writeRow(keyBody, 0, keyBody.length, valueBytes, valueLength);
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
            // measures
            for (MeasureDesc measure : cubeSegment.getCubeDesc().getMeasures()) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, measure.getName()));
            }

            MessageType schema = new MessageType(cubeSegment.getName(), types);
            ParquetSpliceWriter writer = new ParquetSpliceWriter.Builder().setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage())//
                    .setPagesPerGroup(KapConfig.getInstanceFromEnv().getParquetPagesPerGroup())//
                    .setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression())//
                    .setConf(config).setType(schema).setPath(getOutputPath()).build();
            return writer;
        }

        // Generate 10-length random string file name
        private Path getOutputPath() {
            Path path = new Path(outputDir, new StringBuffer().append(RandomStringUtils.randomAlphabetic(10)).append(".parquet").toString());
            return path;
        }
    }
}
