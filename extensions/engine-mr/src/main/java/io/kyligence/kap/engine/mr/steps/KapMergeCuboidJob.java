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

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.CuboidReducer;
import org.apache.kylin.engine.mr.steps.LayerReduerNumSizing;

import io.kyligence.kap.storage.parquet.format.ParquetFileOutputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileReader;

public class KapMergeCuboidJob extends KapCuboidJob {
    private boolean skipped = false;

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment cubeSeg = cube.getSegmentById(segmentID);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            logger.info("Starting: " + jobName);
            job = Job.getInstance(getConf(), jobName);

            setJobClasspath(job, cube.getConfig());
            FileInputFormat.setInputPathFilter(job, CuboidPathFilter.class);

            // set inputs
            int folderNum = addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);
            if (folderNum == 0) {
                skipped = true;
                logger.info("{} is skipped because there's no input folder", getOptionValue(OPTION_JOB_NAME));
                return 0;
            }

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);

            // Mapper
            job.setInputFormatClass(ParquetTarballFileInputFormat.class);
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY, ParquetTarballFileReader.ReadStrategy.KV.toString());
            job.setMapperClass(KapMergeCuboidMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setPartitionerClass(ShardCuboidPartitioner.class);

            job.setReducerClass(CuboidReducer.class);
            job.setOutputFormatClass(ParquetFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            // set path for output
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_OUTPUT_DIR, getWorkingDir(cube.getDescriptor().getConfig(), cube, cubeSeg));

            //push down kylin config
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, KylinConfig.getInstanceFromEnv().getConfigAsString());

            LayerReduerNumSizing.setReduceTaskNum(job, cube.getSegmentById(segmentID), getTotalMapInputMB(), -1);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    public static class CuboidPathFilter implements PathFilter {
        public CuboidPathFilter() {
        }

        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            boolean ret = !(name.endsWith(".parquet") || name.endsWith(".inv") || name.endsWith(".fuzzy"));
            return ret;
        }

    }
}
