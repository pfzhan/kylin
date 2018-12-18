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
import org.apache.kylin.job.exception.ExecuteException;

import com.google.common.collect.Lists;

/**
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    public DefaultChainedExecutable() {
        super();
    }

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
            } else if (state == ExecutableState.STOPPED) {
                // the job is paused
                break;
            } else if (state == ExecutableState.ERROR) {
                throw new IllegalStateException(
                        "invalid subtask state, subtask:" + subTask.getName() + ", state:" + subTask.getStatus());
            }
            if (subTask.isRunnable()) {
                return subTask.execute(context);
            }
        }
        return ExecuteResult.createSucceed();

    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        super.onExecuteStart(executableContext);
    }

    @Override
    protected void onExecuteError(ExecuteResult result, ExecutableContext executableContext) {
        super.onExecuteError(result, executableContext);
        notifyUserStatusChange(executableContext, ExecutableState.ERROR);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        NExecutableManager mgr = getManager();

        if (isDiscarded()) {
            setEndTime(result);
            notifyUserStatusChange(executableContext, ExecutableState.DISCARDED);
        } else if (isPaused()) {
            setEndTime(result);
            notifyUserStatusChange(executableContext, ExecutableState.STOPPED);
        } else if (result.succeed()) {
            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;
            boolean hasError = false;
            boolean hasRunning = false;
            boolean hasDiscarded = false;
            for (Executable task : jobs) {
                if (task.getStatus() == ExecutableState.RUNNING) {
                    logger.error(
                            "There shouldn't be a running subtask[jobId: {}, jobName: {}], \n"
                                    + "it might cause endless state, will retry to fetch subtask's state.",
                            task.getId(), task.getName());
                    boolean retryRet = retryFetchTaskStatus(task);
                    if (!retryRet)
                        hasError = true;
                }
                final ExecutableState status = task.getStatus();
                if (status == ExecutableState.ERROR) {
                    hasError = true;
                }
                if (status != ExecutableState.SUCCEED) {
                    allSucceed = false;
                }
                if (status == ExecutableState.RUNNING) {
                    hasRunning = true;
                }
                if (status == ExecutableState.DISCARDED) {
                    hasDiscarded = true;
                }
            }
            if (allSucceed) {
                setEndTime(result);
                updateJobOutput(getProject(), getId(), ExecutableState.SUCCEED, result.getExtraInfo(), null);
                notifyUserStatusChange(executableContext, ExecutableState.SUCCEED);
            } else if (hasError) {
                setEndTime(result);
                updateJobOutput(getProject(), getId(), ExecutableState.ERROR, result.getExtraInfo(), null);
                notifyUserStatusChange(executableContext, ExecutableState.ERROR);
            } else if (hasRunning) {
                //TODO: normal?
                updateJobOutput(getProject(), getId(), ExecutableState.RUNNING, result.getExtraInfo(), null);
            } else if (hasDiscarded) {
                setEndTime(result);
                updateJobOutput(getProject(), getId(), ExecutableState.DISCARDED, result.getExtraInfo(), null);
            } else {
                //TODO: normal?
                updateJobOutput(getProject(), getId(), ExecutableState.READY, result.getExtraInfo(), null);
            }
        } else {
            setEndTime(result);
            updateJobOutput(getProject(), getId(), ExecutableState.ERROR, result.getExtraInfo(), null);
            notifyUserStatusChange(executableContext, ExecutableState.ERROR);
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

    public final AbstractExecutable getTaskByName(String name) {
        for (AbstractExecutable task : subTasks) {
            if (task.getName() != null && task.getName().equalsIgnoreCase(name)) {
                return task;
            }
        }
        return null;
    }

    @Override
    public void addTask(AbstractExecutable executable) {
        executable.setId(getId() + "-" + String.format("%02d", subTasks.size()));
        executable.setParent(this);
        this.subTasks.add(executable);
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
                    + ", mark parent task failed.", getName(), task.getName());
            return false;
        }
        return true;
    }
}
