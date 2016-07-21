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

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.CuboidReducer;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

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
            options.addOption(OPTION_SEGMENT_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME).toUpperCase();

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment cubeSeg = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            logger.info("Starting: " + jobName);
            job = Job.getInstance(getConf(), jobName);

            setJobClasspath(job, cube.getConfig());

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
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            // set path for output
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_OUTPUT_DIR, getWorkingDir(cube.getDescriptor().getConfig(), cube, cubeSeg));

            //push down kylin config
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, KylinConfig.getInstanceFromEnv().getConfigAsString());

            setReduceTaskNum(job, cube.getDescriptor(), 0);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in MergeCuboidJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }
}
