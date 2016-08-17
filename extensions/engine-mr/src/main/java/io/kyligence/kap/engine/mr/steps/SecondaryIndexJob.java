/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.index.SecondaryIndexCombiner;
import io.kyligence.kap.engine.mr.index.SecondaryIndexMapper;
import io.kyligence.kap.engine.mr.index.SecondaryIndexPartitioner;
import io.kyligence.kap.engine.mr.index.SecondaryIndexReducer;

/**
 */
public class SecondaryIndexJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(SecondaryIndexJob.class);

    @Override
    public boolean isSkipped() {
        return false;
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            // Mapper
            IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cube.getSegment(segmentID, SegmentStatusEnum.NEW)).getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);

            job.setMapperClass(SecondaryIndexMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(SecondaryIndexCombiner.class); // for base cuboid shuffle skew, some rowkey aggregates far more records than others

            int columnsNeedIndex = cube.getDescriptor().getRowkey().getColumnsNeedIndex().length;
            setupReducer(output, columnsNeedIndex);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in CuboidJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private void setupReducer(Path output, int numberOfReducers) throws IOException {
        job.setReducerClass(SecondaryIndexReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(SecondaryIndexPartitioner.class);
        job.setNumReduceTasks(numberOfReducers);

        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, output.toString());

        deletePath(job.getConfiguration(), output);
    }

}
