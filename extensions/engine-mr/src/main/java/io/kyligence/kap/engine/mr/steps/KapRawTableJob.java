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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableOutputFormat;

public class KapRawTableJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(KapRawTableJob.class);
    private static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";

    public KapRawTableJob() {
        this.setMapperClass(HiveToRawTableMapper.class);
    }

    @SuppressWarnings("rawtypes")
    private Class<? extends Mapper> mapperClass;

    private boolean skipped = false;

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (this.mapperClass == null)
            throw new Exception("Mapper class is not set!");

        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_INPUT_FORMAT);
            options.addOption(OPTION_CUBING_JOB_ID);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String rawTableName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            RawTableManager rawMgr = RawTableManager.getInstance(config);
            RawTableInstance rawInstance = rawMgr.getRawTableInstance(rawTableName);
            RawTableSegment rawSeg = rawInstance.getSegmentById(segmentID);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, rawInstance.getConfig());

            // Mapper
            int numFiles = configureMapperInputFormat(rawInstance.getSegmentById(segmentID));
            if (numFiles == 0) {
                skipped = true;
                logger.info("{} is skipped because there's no input file", getOptionValue(OPTION_JOB_NAME));
                return 0;
            }

            job.setMapperClass(this.mapperClass);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(KylinReducer.class); // for base cuboid shuffle skew, some rowkey aggregates far more records than others

            // Reducer
            job.setReducerClass(KylinReducer.class);
            job.setOutputFormatClass(ParquetRawTableOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, output);
            // Partitioner
            job.setPartitionerClass(ShardPartitioner.class);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, rawTableName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            job.getConfiguration().set(ParquetFormatConstants.KYLIN_OUTPUT_DIR, getWorkingDir(config, rawInstance, rawSeg));
            // add metadata to distributed cache
            attachKylinPropsAndMetadata(rawSeg, job.getConfiguration());

            if (rawSeg.getShardNum() > 0)
                job.getConfiguration().setInt(MAPRED_REDUCE_TASKS, rawSeg.getShardNum());

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    public static String getWorkingDir(KylinConfig config, RawTableInstance instance, RawTableSegment seg) {
        return new StringBuffer(KapConfig.wrap(config).getParquentStoragePath()).append(instance.getUuid()).append("/").append(seg.getUuid()).append("/").toString();
    }

    private int configureMapperInputFormat(RawTableSegment seg) throws IOException {
        String input = getOptionValue(OPTION_INPUT_PATH);

        if ("FLAT_TABLE".equals(input)) {
            // TODO: getBatchCubingInputSide should support RawTableSegment too
            IMRInput.IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(seg.getCubeSegment()).getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);
            return 1; //return a non-zero value
        } else
            return 0;
    }

    private void attachKylinPropsAndMetadata(RawTableSegment rawSegment, Configuration conf) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(rawSegment.getConfig());
        RawTableInstance instance = rawSegment.getRawTableInstance();
        // write raw/cube / model_desc / raw_desc / dict / table
        ArrayList<String> dumpList = new ArrayList<String>();
        dumpList.add(instance.getResourcePath());
        dumpList.add(instance.getRawTableDesc().getModel().getResourcePath());
        dumpList.add(instance.getRawTableDesc().getResourcePath());

        for (String tableName : instance.getRawTableDesc().getModel().getAllTables()) {
            TableDesc table = metaMgr.getTableDesc(tableName);
            dumpList.add(table.getResourcePath());
            List<String> dependentResources = SourceFactory.getMRDependentResources(table);
            dumpList.addAll(dependentResources);
        }
        CubeInstance cube = rawSegment.getCubeSegment().getCubeInstance();
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getResourcePath());
        attachKylinPropsAndMetadata(dumpList, instance.getConfig(), conf);
    }

    /**
     * @param mapperClass
     *            the mapperClass to set
     */
    @SuppressWarnings("rawtypes")
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }
}
