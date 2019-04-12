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

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.exception.JobSuicideException;

import com.google.common.collect.Lists;

/**
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    public DefaultChainedExecutable() {
        super();
    }

    @Override
    public void initConfig(KylinConfig config) {
        super.initConfig(config);
        for (AbstractExecutable sub : subTasks) {
            sub.initConfig(config);
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        final int size = executables.size();
        for (int i = 0; i < size; ++i) {
            Executable subTask = executables.get(i);
            ExecutableState state = subTask.getStatus();
            if (state == ExecutableState.RUNNING) {
                // there is already running subtask, no need to start a new subtask
                break;
            } else if (state == ExecutableState.PAUSED) {
                // the job is paused
                break;
            } else if (state == ExecutableState.ERROR) {
                throw new IllegalStateException(
                        "invalid subtask state, subtask:" + subTask.getDisplayName() + ", state:" + subTask.getStatus());
            }
            if (subTask.isRunnable()) {
                try {
                    subTask.execute(context);
                } catch (JobSuicideException e) {
                    return ExecuteResult.createSucceed();
                } catch (JobStoppedException e) {
                    return ExecuteResult.createSucceed();
                } catch (ExecuteException e) {
                    if (e.getCause() instanceof JobSuicideException) {
                        return ExecuteResult.createSucceed();
                    }
                    throw e;
                }
            }
        }
        return ExecuteResult.createSucceed();

    }

    @Override
    protected void onExecuteError(ExecuteResult result, ExecutableContext executableContext) {
        super.onExecuteError(result, executableContext);
        notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        if (isDiscarded() || isPaused()) {
            return;
        }
        if (!result.succeed()) {
            updateJobOutput(getProject(), getId(), ExecutableState.ERROR, null, this::onExecuteErrorHook);
            notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
            return;
        }
        List<? extends Executable> jobs = getTasks();
        boolean allSucceed = true;
        boolean hasError = false;
        boolean hasDiscarded = false;
        boolean hasSuicidal = false;
        for (Executable task : jobs) {
            switch (task.getStatus()) {
                case RUNNING:
                    logger.error(
                            "There shouldn't be a running subtask[{}], \n"
                                    + "it might cause endless state, will retry to fetch subtask's state.",
                            task.getDisplayName());
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
            updateJobOutput(getProject(), getId(), ExecutableState.SUCCEED, null);
        } else if (hasError) {
            updateJobOutput(getProject(), getId(), ExecutableState.ERROR, null, this::onExecuteErrorHook);
            notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
        } else if (hasDiscarded) {
            updateJobOutput(getProject(), getId(), ExecutableState.DISCARDED, null);
        } else if (hasSuicidal) {
            updateJobOutput(getProject(), getId(), ExecutableState.SUICIDAL, null);
        } else {
            //TODO: normal?
            updateJobOutput(getProject(), getId(), ExecutableState.READY, null);
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


    private boolean retryFetchTaskStatus(Executable task) {
        boolean hasRunning = false;
        int retry = 1;
        while (retry <= 10) {
            ExecutableState retryState = task.getStatus();
            if (retryState == ExecutableState.RUNNING) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    logger.error("Failed to Sleep: ", e);
                    Thread.currentThread().interrupt();
                }
                hasRunning = true;
                logger.error("With {} times retry, it's state is still RUNNING", retry);
            } else {
                logger.info("With {} times retry, status is changed to: {}", retry, retryState);
                hasRunning = false;
                break;
            }
            retry++;
        }
        if (hasRunning) {
            logger.error("Parent task: {} is finished, but it's subtask: {}'s state is still RUNNING \n"
                    + ", mark parent task failed.", getDisplayName(), task.getDisplayName());
            return false;
        }
        return true;
    }
}
