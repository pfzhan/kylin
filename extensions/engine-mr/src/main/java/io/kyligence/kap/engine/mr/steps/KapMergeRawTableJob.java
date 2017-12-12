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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableOutputFormat;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;

public class KapMergeRawTableJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(KapMergeRawTableJob.class);

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

            RawTableManager rawMgr = RawTableManager.getInstance(KylinConfig.getInstanceFromEnv());
            RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);
            RawTableSegment rawSeg = raw.getSegmentById(segmentID);
            CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeManager.getCube(cubeName);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            logger.info("Starting: " + jobName);
            job = Job.getInstance(getConf(), jobName);

            setJobClasspath(job, raw.getConfig());

            // set inputs
            int folderNum = addRawInputDirs(getOptionValue(OPTION_INPUT_PATH), job);
            if (folderNum == 0) {
                skipped = true;
                logger.info("{} is skipped because there's no input folder", getOptionValue(OPTION_JOB_NAME));
                return 0;
            }

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);

            // Mapper
            job.setInputFormatClass(ParquetRawTableInputFormat.class);
            job.setMapperClass(KapMergeRawTableMapper.class);
            job.setMapOutputKeyClass(ByteArrayListWritable.class);
            job.setMapOutputValueClass(ByteArrayListWritable.class);
            job.setCombinerClass(KylinReducer.class);
            job.setPartitionerClass(RawTablePartitioner.class);

            // Reducer
            job.setReducerClass(KylinReducer.class);
            job.setOutputFormatClass(ParquetRawTableOutputFormat.class);
            job.setOutputKeyClass(ByteArrayListWritable.class);
            job.setOutputValueClass(ByteArrayListWritable.class);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(rawSeg, cube, job.getConfiguration());

            //push down kylin config
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, KylinConfig.getInstanceFromEnv().exportAllToString());

            if (raw.getShardNumber() > 0)
                job.setNumReduceTasks(raw.getShardNumber());

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private void attachKylinPropsAndMetadata(RawTableSegment rawSegment, CubeInstance cube, Configuration conf) throws IOException {
        RawTableInstance instance = rawSegment.getRawTableInstance();
        // write raw/cube / model_desc / raw_desc / dict / table
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.add(instance.getResourcePath());
        dumpList.add(instance.getRawTableDesc().getModel().getResourcePath());
        dumpList.add(instance.getRawTableDesc().getResourcePath());
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getResourcePath());

        for (TableRef tableRef : instance.getRawTableDesc().getModel().getAllTables()) {
            TableDesc table = tableRef.getTableDesc();
            dumpList.add(table.getResourcePath());
            List<String> dependentResources = SourceFactory.getMRDependentResources(table);
            dumpList.addAll(dependentResources);
        }
        dumpKylinPropsAndMetadata(cube.getProject(), dumpList, instance.getConfig(), conf);
    }

    public int addRawInputDirs(String input, Job job) throws IOException {
        int folderNum = addRawInputDirs(StringSplitter.split(input, ","), job);
        logger.info("Number of added folders:" + folderNum);
        return folderNum;
    }

    public int addRawInputDirs(String[] inputs, Job job) throws IOException {
        int ret = 0;//return number of added folders
        for (String inp : inputs) {
            inp = inp.trim();
            if (inp.endsWith("/*")) {
                inp = inp.substring(0, inp.length() - 2);
                FileSystem fs = HadoopUtil.getWorkingFileSystem(job.getConfiguration());
                Path path = new Path(inp);

                if (!fs.exists(path)) {
                    logger.warn("Path not exist:" + path.toString());
                    continue;
                }

                FileStatus[] fileStatuses = fs.listStatus(path);
                for (FileStatus stat : fileStatuses) {
                    if (stat.getPath().getName().endsWith(".parquet"))
                        ret += addRawInputDirs(new String[] { stat.getPath().toString() }, job);
                }
            } else {
                logger.info("Add input " + inp);
                FileInputFormat.addInputPath(job, new Path(inp));
                ret++;
            }
        }
        return ret;
    }
}
