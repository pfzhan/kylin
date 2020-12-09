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
package org.apache.kylin.job.runners;

import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.job.exception.JobStoppedVoluntarilyException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class JobRunner extends AbstractDefaultSchedulerRunner {
    private static final Logger logger = LoggerFactory.getLogger(JobRunner.class);

    private final AbstractExecutable executable;

    private final FetcherRunner fetcherRunner;

    public JobRunner(NDefaultScheduler nDefaultScheduler, AbstractExecutable executable, FetcherRunner fetcherRunner) {
        super(nDefaultScheduler);
        this.fetcherRunner = fetcherRunner;
        this.executable = executable;
    }

    @Override
    protected void doRun() {
        //only the first 8 chars of the job uuid
        try (SetThreadName ignored = new SetThreadName("JobWorker(project:%s,jobid:%s)", project,
                executable.getId().substring(0, 8))) {
            executable.execute(context);
            // trigger the next step asap
            fetcherRunner.scheduleNext();
        } catch (JobStoppedVoluntarilyException | JobStoppedNonVoluntarilyException e) {
            logger.info("Job quits either voluntarily or non-voluntarily", e);
        } catch (ExecuteException e) {
            logger.error("ExecuteException occurred while job: " + executable.getId(), e);
        } catch (Exception e) {
            logger.error("unknown error execute job: " + executable.getId(), e);
        } finally {
            context.removeRunningJob(executable);
            val config = KylinConfig.getInstanceFromEnv();
            var usingMemory = 0;
            if (!config.getSparkMaster().equals("yarn-cluster")) {
                usingMemory = executable.computeStepDriverMemory();
            }
            logger.info("Before global memory release {}", NDefaultScheduler.getMemoryRemaining().availablePermits());
            NDefaultScheduler.getMemoryRemaining().release(usingMemory);
            logger.info("After global memory release {}", NDefaultScheduler.getMemoryRemaining().availablePermits());
        }
    }
}
