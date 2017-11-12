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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateOutputDirStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(UpdateOutputDirStep.class);
    private final String OUTPUT_DIR = "output_dir";
    private final String SUBDIR_FILTER = "subdir_filter";
    private final String JOB_ID = "cube_job_id";
    private final String CHECK_SKIP = "check_skip";
    private final String CHECK_AlGORITHM = "check_algorithm";

    public String getOutputDir() {
        return getParam(OUTPUT_DIR);
    }

    public void setOutputDir(String cubeOutputDir) {
        setParam(OUTPUT_DIR, cubeOutputDir);
    }

    public String getSubdirFilter() {
        return getParam(SUBDIR_FILTER);
    }

    public void setSubdirFilter(String subdir) {
        setParam(SUBDIR_FILTER, subdir);
    }

    public String getJobId() {
        return getParam(JOB_ID);
    }

    public void setJobId(String jobId) {
        setParam(JOB_ID, jobId);
    }

    public void setCheckSkip(boolean check) {
        setParam(CHECK_SKIP, String.valueOf(check));
    }

    public void setCheckAlgorithm(String alg) {
        setParam(CHECK_AlGORITHM, alg);
    }

    public CubingJob.AlgorithmEnum getCheckAlgorithm() {
        String alg = getParam(CHECK_AlGORITHM);
        return CubingJob.AlgorithmEnum.valueOf(alg);
    }

    public boolean getCheckSkip() {
        String checkSkip = getParam(CHECK_SKIP);
        if (checkSkip == null) {
            return false;
        }

        return Boolean.valueOf(checkSkip);
    }

    private boolean checkSkip(String cubingJobId) {
        if (cubingJobId == null)
            return false;

        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubingJob cubingJob = (CubingJob) execMgr.getJob(cubingJobId);
        return cubingJob.getAlgorithm() != getCheckAlgorithm();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) {
        if (getCheckSkip() && checkSkip(getJobId())) {
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        }
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Path cubeOutputPath = new Path(getOutputDir());
            FileStatus outputDirStatus = fs.getFileStatus(cubeOutputPath);
            assert (outputDirStatus.isDirectory());
            FileStatus[] childFileStatus = fs.listStatus(cubeOutputPath);
            for (FileStatus status : childFileStatus) {
                if (status.getPath().getName().startsWith(getSubdirFilter())) {
                    copyMergeDirs(fs, status.getPath(), cubeOutputPath);
                    fs.delete(status.getPath(), true);
                }
            }
        } catch (IOException e) {
            logger.error("{}", e);
            return new ExecuteResult(ExecuteResult.State.FAILED);
        }
        return new ExecuteResult(ExecuteResult.State.SUCCEED);
    }

    private void copyMergeDirs(FileSystem fs, Path src, Path dest) throws IOException {
        FileStatus[] childFileStatus = fs.listStatus(src);
        for (FileStatus status : childFileStatus) {
            String name = status.getPath().getName();
            Path destChild = new Path(dest, name);
            if (fs.exists(destChild) && fs.isDirectory(status.getPath()) && fs.isDirectory(destChild)) {
                copyMergeDirs(fs, status.getPath(), destChild);
            } else {
                fs.rename(status.getPath(), destChild);
            }
        }
    }
}
