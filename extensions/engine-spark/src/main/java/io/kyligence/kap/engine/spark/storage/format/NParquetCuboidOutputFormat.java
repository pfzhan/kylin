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

package io.kyligence.kap.engine.spark.storage.format;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dict.DateStrDictionary;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.cube.model.NColumnFamilyDesc;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.mr.common.KapBatchConstants;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.storage.parquet.format.ParquetOrderedFileWriter;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;

/**
 * cube build output format
 */
public class NParquetCuboidOutputFormat extends FileOutputFormat<Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(NParquetCuboidOutputFormat.class);

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new NParquetCubeWriter((FileOutputCommitter) this.getOutputCommitter(job), job, job.getOutputKeyClass(),
                job.getOutputValueClass());
    }

    public static class NParquetCubeWriter extends ParquetOrderedFileWriter<Text, Text> {

        private int curShardId = -1;
        private File curLocalParquet = null;
        private FSDataOutputStream curOutStream;
        private int lineCounter = 0;

        private Configuration localConfig;
        private Path outputDir = null;
        private KylinConfig kylinConfig;
        private RowKeyColumnIO rowKeyColumnIO;
        private MeasureCodec measureCodec;
        private NDataflow dataflow;
        private NDataSegment dataSegment;
        private NCubePlan cubePlan;
        private NCuboidLayout cuboidLayout;
        private BiMap<Integer, TblColRef> orderedDimensions;
        private BiMap<Integer, NDataModel.Measure> orderedMeasures;

        private ParquetPageIndexWriter indexBundleWriter;

        private ParquetPageIndexWriter newIndexWriter() throws IOException, InterruptedException {
            double spillThresholdMB = KapConfig.wrap(dataSegment.getConfig()).getParquetPageIndexSpillThresholdMB();
            double rowGroupMB = 64D; // TODO: will make this configurable
            spillThresholdMB = spillThresholdMB + rowGroupMB;

            int columnNum = orderedDimensions.size();
            int[] columnLength = new int[columnNum];
            int[] cardinality = new int[columnNum];
            String[] columnName = new String[columnNum];
            boolean[] onlyEQIndex = new boolean[columnNum];

            Map<Integer, String> indexMap = cuboidLayout.getDimensionIndexMap();
            int col = 0;
            for (Map.Entry<Integer, TblColRef> dim : orderedDimensions.entrySet()) {
                int colCardinality = -1;
                Dictionary<String> dict = rowKeyColumnIO.getDictionary(dim.getValue());
                if (dict != null) {
                    colCardinality = dict.getSize();
                    if (dict instanceof DateStrDictionary) {
                        colCardinality = -1;
                    }
                }
                cardinality[col] = colCardinality;
                onlyEQIndex[col] = "eq".equalsIgnoreCase(indexMap.get(dim.getKey()));
                columnLength[col] = rowKeyColumnIO.getColumnLength(dim.getValue());
                columnName[col] = dim.getValue().getIdentity();

                col++;
            }

            return new ParquetPageIndexWriter(columnName, columnLength, cardinality, onlyEQIndex, curOutStream,
                    spillThresholdMB);
        }

        public NParquetCubeWriter(FileOutputCommitter committer, TaskAttemptContext context, Class<?> keyClass,
                Class<?> valueClass) throws IOException, InterruptedException {
            this.outputDir = committer.getWorkPath();
            this.localConfig = HadoopUtil.newLocalConfiguration();

            logger.info("Attempt output dir: {}, Task output dir: {}", outputDir,
                    committer.getCommittedTaskPath(context));

            kylinConfig = AbstractHadoopJob
                    .loadKylinConfigFromHdfsIfNeeded(context.getConfiguration().get(NBatchConstants.P_DIST_META_URL));

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            int segmentID = Integer.valueOf(context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID));
            String projectName = context.getConfiguration().get(BatchConstants.CFG_PROJECT_NAME);
            long cuboidLayoutId = Long
                    .valueOf(context.getConfiguration().get(KapBatchConstants.KYLIN_CUBOID_LAYOUT_ID));
            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);
            dataflow = NDataflowManager.getInstance(kylinConfig, projectName).getDataflow(cubeName);
            cubePlan = dataflow.getCubePlan();
            dataSegment = dataflow.getSegment(segmentID);
            cuboidLayout = cubePlan.getSpanningTree().getCuboidLayout(cuboidLayoutId);
            orderedDimensions = cuboidLayout.getOrderedDimensions();
            orderedMeasures = cuboidLayout.getOrderedMeasures();
            measureCodec = new MeasureCodec(orderedMeasures.values().toArray(new MeasureDesc[0]));
            rowKeyColumnIO = new RowKeyColumnIO(new NCubeDimEncMap(dataSegment));

            if (keyClass != Text.class || valueClass != Text.class) {
                throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
            }
        }

        @Override
        protected void freshWriter(Text key, Text value) throws InterruptedException, IOException {
            int shardId = TaskContext.getPartitionId();

            if (shardId != curShardId) { // meet new shard, need to refresh writers
                cleanWriter();

                curShardId = shardId;
                lineCounter = 0;

                Path curOutPath = getOutputPath();
                logger.info("Meet a new shard: shard={}, path={}", curShardId, curOutPath.toString());
                curOutStream = HadoopUtil.getFileSystem(curOutPath).create(curOutPath);

                writer = newWriter();
                indexBundleWriter = newIndexWriter();
            }
        }

        @Override
        protected void cleanWriter() throws IOException {
            logger.info("Finish written {} lines for shard {}", lineCounter, curShardId);
            if (writer != null) {
                writer.close(); // save parquet to local tmp file
            }

            if (indexBundleWriter != null) { // flush header part (offset+index)
                indexBundleWriter.stopWrite();

                long indexSize = indexBundleWriter.getTotalSize();
                curOutStream.writeLong(8 + indexSize);

                logger.info("Start to flush columnar index: size={}", indexSize);
                indexBundleWriter.flush();
                indexBundleWriter = null;
            }

            if (writer != null) {
                try (InputStream is = FileUtils.openInputStream(curLocalParquet)) {
                    long size = IOUtils.copyLarge(is, curOutStream);
                    logger.info("Flushed columnar data: from={}, size={}", curLocalParquet.getAbsolutePath(), size);
                }
                FileUtils.deleteQuietly(curLocalParquet);

                curOutStream.close();
                writer = null;
            }
        }

        @Override
        protected ParquetRawWriter newWriter() throws IOException, InterruptedException {
            NColumnFamilyDesc[] dimCFs = cuboidLayout.getDimensionCFs();
            NColumnFamilyDesc[] measureCFs = cuboidLayout.getMeasureCFs();

            List<Type> types = Lists.newArrayListWithExpectedSize(dimCFs.length + measureCFs.length);
            for (NColumnFamilyDesc cf : dimCFs) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
                        cf.getName()));
            }
            for (NColumnFamilyDesc cf : measureCFs) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
                        cf.getName()));
            }

            curLocalParquet = File.createTempFile(UUID.randomUUID().toString(), ".parquet");
            FileUtils.forceDelete(curLocalParquet);

            MessageType schema = new MessageType(dataSegment.getName(), types);
            KapConfig segKapConfig = KapConfig.wrap(dataSegment.getConfig());
            return new ParquetRawWriter.Builder().setRowsPerPage(segKapConfig.getParquetRowsPerPage())
                    .setPagesPerGroup(segKapConfig.getParquetPagesPerGroup())
                    .setCodecName(segKapConfig.getParquetPageCompression()).setConf(localConfig).setType(schema)
                    .setPath(new Path(curLocalParquet.getAbsolutePath())).build();
        }

        @Override
        protected void writeData(Text key, Text value) throws IOException {
            byte[] bytes = key.getBytes().clone();

            NColumnFamilyDesc[] dimCFs = cuboidLayout.getDimensionCFs();
            NColumnFamilyDesc[] measureCFs = cuboidLayout.getMeasureCFs();
            int[] valueLengths = new int[dimCFs.length + measureCFs.length];
            int i = 0;
            int measureOffset = 0;
            for (NColumnFamilyDesc dimCF : dimCFs) {
                valueLengths[i] = 0;
                for (int c : dimCF.getColumns()) {
                    valueLengths[i] += rowKeyColumnIO.getColumnLength(orderedDimensions.get(c));
                }
                measureOffset += valueLengths[i];
                i++;
            }

            ByteBuffer measureBuf = ByteBuffer.wrap(bytes);
            measureBuf.position(measureOffset);
            int[] valueLength = measureCodec.getPeekLength(measureBuf);
            int measureIdx = 0;
            for (NColumnFamilyDesc measureCF : measureCFs) {
                valueLengths[i] = 0;
                for (int c : measureCF.getColumns()) {
                    valueLengths[i] += valueLength[measureIdx++];
                }
                i++;
            }
            writer.writeRow(bytes, valueLengths);
            indexBundleWriter.write(bytes, writer.getPageCntSoFar());
            lineCounter++;
        }

        @Override
        protected Path getOutputPath() {
            return new Path(outputDir, curShardId + ".parquettar");
        }
    }
}
