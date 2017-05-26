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
import java.security.InvalidParameterException;
import java.util.ArrayList;
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
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.BufferedRawColumnCodec;
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
    public RecordWriter<ByteArrayListWritable, ByteArrayListWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ParquetRawTableFileWriter((FileOutputCommitter) this.getOutputCommitter(job), job, job.getOutputKeyClass(), job.getOutputValueClass());
    }

    public static class ParquetRawTableFileWriter extends ParquetOrderedFileWriter<ByteArrayListWritable, ByteArrayListWritable> {
        private static final Logger logger = LoggerFactory.getLogger(ParquetRawTableFileWriter.class);

        private short curShardId = -1;
        private int curShardCounter = 0;

        private Configuration config;
        private KylinConfig kylinConfig;
        private RawTableInstance rawTableInstance;
        private RawTableDesc rawTableDesc;
        private BufferedRawColumnCodec rawColumnsCodec;
        private Path outputDir = null;

        public ParquetRawTableFileWriter(FileOutputCommitter committer, TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException, InterruptedException {
            this.config = context.getConfiguration();
            this.outputDir = committer.getTaskAttemptPath(context);

            logger.info("tmp output dir: {}", outputDir);

            kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

            String rawName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
            logger.info("RawTableName is " + rawName + " and segmentID is " + segmentID);
            rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(rawName);
            rawTableDesc = rawTableInstance.getRawTableDesc();
            GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableDesc);
            rawColumnsCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());

            // FIXME: Text involves array copy every time
            if (keyClass == Text.class && valueClass == Text.class) {
                logger.info("KV class is Text");
            } else {
                throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
            }
        }

        @Override
        public void write(ByteArrayListWritable key, ByteArrayListWritable value) throws IOException, InterruptedException {
            super.write(key, value);
        }

        @Override
        protected void freshWriter(ByteArrayListWritable key, ByteArrayListWritable value) throws IOException, InterruptedException {
            short shardId = BytesUtil.readShort(key.get().get(0));

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
            List<TblColRef> columns = rawTableDesc.getColumnsInOrder();

            for (TblColRef column : columns) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, column.getName()));
            }

            MessageType schema = new MessageType(rawTableDesc.getName(), types);
            rawWriter = new ParquetRawWriter.Builder().setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage())//
                    .setPagesPerGroup(KapConfig.getInstanceFromEnv().getParquetPagesPerGroup())//
                    .setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression()).setConf(config).setType(schema).setPath(getOutputPath()).build();
            return rawWriter;
        }

        @Override
        protected void writeData(ByteArrayListWritable key, ByteArrayListWritable value) throws IOException {
            List<byte[]> sortby = key.get().subList(1, key.get().size());
            List<byte[]> nonSortby = value.get();
            writer.writeRow(sortby, nonSortby);
        }

        @Override
        protected Path getOutputPath() {
            Path path = new Path(outputDir, new StringBuffer().append(RawTableConstants.RawTableDir).append("/").append(curShardId).append(".parquet").toString());
            return path;
        }
    }
}
