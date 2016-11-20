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

package io.kyligence.kap.storage.parquet.steps;

import static io.kyligence.kap.engine.mr.steps.ParquertMRJobUtils.addParquetInputFile;

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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
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
import io.kyligence.kap.storage.parquet.format.ParquetRawTablePageInputFormat;

public class RawTablePageIndexJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(RawTablePageIndexJob.class);

    protected boolean skipped = false;

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
            options.addOption(OPTION_CUBING_JOB_ID);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            RawTableManager rawMgr = RawTableManager.getInstance(KylinConfig.getInstanceFromEnv());
            RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

            CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeManager.getCube(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            setJobClasspath(job, raw.getConfig());

            int inputNum = setJobInputFile(job, new Path(getOptionValue(OPTION_INPUT_PATH)));
            if (inputNum == 0) {
                skipped = true;
                logger.info("ParquetPageIndexJob is skipped because there's no input file");
                return 0;
            }

            setMapperClass(job);
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setInputFormatClass(ParquetRawTablePageInputFormat.class);
            job.setNumReduceTasks(0);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(raw.getSegmentById(segmentID), cube, job.getConfiguration());
            return waitForCompletion(job);
        } finally {
            if (job != null) {
                cleanupTempConfFile(job.getConfiguration());
            }
        }
    }

    protected void setMapperClass(Job job) {
        job.setMapperClass(RawTablePageIndexMapper.class);
    }

    private void attachKylinPropsAndMetadata(RawTableSegment rawSegment, CubeInstance cube, Configuration conf) throws IOException {
        RawTableInstance instance = rawSegment.getRawTableInstance();
        // write raw / model_desc / raw_desc / dict / table
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
        attachKylinPropsAndMetadata(dumpList, instance.getConfig(), conf);
    }

    protected int setJobInputFile(Job job, Path path) throws IOException {
        int ret = 0;
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (!fs.exists(path)) {
            logger.warn("Input {} does not exist.", path.toString());
        } else if (fs.isDirectory(path)) {
            for (FileStatus fileStatus : fs.listStatus(path)) {
                // build index for only rawtable file
                if (!fileStatus.getPath().getName().matches("^\\d+$")) {
                    ret += addParquetInputFile(job, fileStatus.getPath());
                }
            }
        } else {
            logger.warn("Input Path: {} should be directory", path);
        }
        return ret;
    }
}
