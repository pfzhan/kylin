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
            IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cube.getSegmentById(segmentID)).getFlatTableInputFormat();
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
