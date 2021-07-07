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

package io.kyligence.kap.secondstorage.test.utils;

import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageModelCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public interface JobWaiter {
    default void waitJobFinish(String project, String jobId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        DefaultChainedExecutable job = (DefaultChainedExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
        val firstErrorMsg = job.getTasks().stream()
                .filter(abstractExecutable -> abstractExecutable.getStatus() == ExecutableState.ERROR).findFirst()
                .map(task -> {
                    try (InputStream verboseMsgStream = executableManager
                            .getOutputFromHDFSByJobId(job.getId(), task.getId(), Integer.MAX_VALUE)
                            .getVerboseMsgStream();
                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(verboseMsgStream, Charset.defaultCharset()))) {

                        String line;
                        StringBuilder sampleData = new StringBuilder();
                        while ((line = reader.readLine()) != null) {
                            if (sampleData.length() > 0) {
                                sampleData.append('\n');
                            }
                            sampleData.append(line);
                        }

                        return sampleData.toString();
                    } catch (IOException e) {
                        return null;
                    }
                }).orElse("Unknown Error");
        Assert.assertEquals(firstErrorMsg, ExecutableState.SUCCEED, executableManager.getJob(jobId).getStatus());
    }

    default String triggerClickHouseLoadJob(String project, String modelId, String userName, List<String> segIds) {
        AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.of(project, modelId, userName, segIds.stream());
        ExecutableUtil.computeParams(jobParam);
        localHandler.handle(jobParam);
        return jobParam.getJobId();
    }

    default String triggerModelCleanJob(String project, String modelId, String userName) {
        AbstractJobHandler handler = new SecondStorageModelCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.modelCleanParam(project, modelId, userName);
        ExecutableUtil.computeParams(param);
        handler.handle(param);
        return param.getJobId();
    }
}