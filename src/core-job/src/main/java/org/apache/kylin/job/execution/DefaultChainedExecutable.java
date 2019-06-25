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

package org.apache.kylin.job.execution;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;

/**
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    public DefaultChainedExecutable() {
        super();
    }

    public Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        for (Executable subTask : executables) {
            if (subTask.isRunnable()) {
                subTask.execute(context);
            } else if (subTask.getStatus() == ExecutableState.SUCCEED) {
                logger.info("Subtask {} was succeed, skip it", subTask.getDisplayName());
            } else {
                throw new IllegalStateException("invalid subtask state, sub task:" + subTask.getDisplayName()
                        + ", state:" + subTask.getStatus());
            }
        }
        return ExecuteResult.createSucceed();

    }

    @Override
    protected boolean needCheckState() {
        return false;
    }

    protected void onExecuteError(ExecuteResult result) throws JobStoppedException {
        super.onExecuteError(result);
        notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
        NMetricsGroup.counterInc(NMetricsName.JOB_ERROR, NMetricsCategory.PROJECT, getProject());
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result) throws JobStoppedException {

        Preconditions.checkState(result.succeed());

        wrapWithCheckQuit(() -> {

            if (isStoppedNonVoluntarily())
                return;

            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;
            boolean hasError = false;
            boolean hasDiscarded = false;
            boolean hasSuicidal = false;
            for (Executable task : jobs) {
                switch (task.getStatus()) {
                case RUNNING:
                    hasError = true;
                    break;
                case ERROR:
                    hasError = true;
                    break;
                case DISCARDED:
                    hasDiscarded = true;
                    break;
                case SUICIDAL:
                    hasSuicidal = true;
                    break;
                default:
                    break;
                }
                final ExecutableState status = task.getStatus();
                if (status != ExecutableState.SUCCEED) {
                    allSucceed = false;
                }
            }
            if (allSucceed) {
                updateJobOutput(getProject(), getId(), ExecutableState.SUCCEED, null, null, null);
            } else if (hasError) {
                logger.warn("[UNEXPECTED_THINGS_HAPPENED] Unexpected ERROR state discovered here!!!");
                updateJobOutput(getProject(), getId(), ExecutableState.ERROR, null, null, this::onExecuteErrorHook);
                notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
            } else if (hasDiscarded) {
                updateJobOutput(getProject(), getId(), ExecutableState.DISCARDED, null, null, null);
            } else if (hasSuicidal) {
                updateJobOutput(getProject(), getId(), ExecutableState.SUICIDAL, null, null, null);
            } else {
                logger.warn("[UNEXPECTED_THINGS_HAPPENED] Unexpected READY state discovered here!!!");
                updateJobOutput(getProject(), getId(), ExecutableState.READY, null, null, null);
            }
        });

        // dispatch job-finished message out
        SchedulerEventBusFactory.getInstance(KylinConfig.getInstanceFromEnv())
                .postWithLimit(new JobFinishedNotifier(getProject()));

        ExecutableState state = getStatus();
        if (state != null && state.isFinalState()) {
            NMetricsGroup.counterInc(NMetricsName.JOB_FINISHED, NMetricsCategory.PROJECT, getProject());
            NMetricsGroup.counterInc(NMetricsName.JOB_DURATION, NMetricsCategory.PROJECT, getProject(), getDuration());
            NMetricsGroup.histogramUpdate(NMetricsName.JOB_DURATION_HISTOGRAM, NMetricsCategory.PROJECT, getProject(),
                    getDuration());
        }

    }

    @Override
    public List<AbstractExecutable> getTasks() {
        return subTasks;
    }

    @Override
    protected boolean needRetry() {
        return false;
    }

    @Override
    public void addTask(AbstractExecutable executable) {
        executable.setId(getId() + "_" + String.format("%02d", subTasks.size()));
        executable.setParent(this);
        this.subTasks.add(executable);
    }

    @Override
    public <T extends AbstractExecutable> T getTask(Class<T> clz) {
        List<AbstractExecutable> tasks = getTasks();
        for (AbstractExecutable task : tasks) {
            if (task.getClass().equals(clz)) {
                return (T) task;
            }
        }
        return null;
    }

}
