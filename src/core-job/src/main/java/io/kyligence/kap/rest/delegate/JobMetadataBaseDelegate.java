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

package io.kyligence.kap.rest.delegate;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.manager.JobManager;
import lombok.val;

public class JobMetadataBaseDelegate {

    private <T> T getManager(Class<T> clz, String project) {
        return KylinConfig.getInstanceFromEnv().getManager(project, clz);
    }

    public String addSegmentJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.addSegmentJob(jobMetadataRequest.parseJobParam());
    }

    public String addIndexJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.addIndexJob(jobMetadataRequest.parseJobParam());
    }

    public Set<Long> getLayoutsByRunningJobs(String project, String modelId) {
        List<AbstractExecutable> runningJobList = ExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).getPartialExecutablesByStatusList(
                        Sets.newHashSet(ExecutableState.READY, ExecutableState.PENDING, ExecutableState.RUNNING,
                                ExecutableState.PAUSED, ExecutableState.ERROR), //
                        modelId);

        return runningJobList.stream()
                .filter(abstractExecutable -> Objects.equals(modelId, abstractExecutable.getTargetSubject()))
                .map(AbstractExecutable::getToBeDeletedLayoutIds).flatMap(Set::stream).collect(Collectors.toSet());
    }

    public long countByModelAndStatus(String project, String model, String status, JobTypeEnum... jobTypes) {
        Predicate<ExecutableState> predicate = null;
        if (status.equals("isProgressing")) {
            predicate = ExecutableState::isProgressing;
        } else if (status.equals("RUNNING")) {
            predicate = state -> state == ExecutableState.RUNNING;
        }
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).countByModelAndStatus(model,
                predicate, jobTypes);
    }

    public List<ExecutablePO> getJobExecutablesPO(String project) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getAllJobs();
    }

    public List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state, JobTypeEnum... jobTypes) {
        Predicate<ExecutableState> predicate = null;
        if (state.equals("isRunning")) {
            predicate = ExecutableState::isRunning;
        }
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listExecPOByJobTypeAndStatus(predicate, jobTypes);
    }

    public List<ExecutablePO> getExecutablePOsByStatus(String project, ExecutableState... status) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getExecutablePOsByStatus(Lists.newArrayList(status));
    }

    public void deleteJobByIdList(String project, List<String> jobIdList) {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).deleteJobByIdList(jobIdList);
    }

    public void discardJob(String project, String jobId) {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).discardJob(jobId);
    }

    public void clearJobsByProject(String project) {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).deleteAllJobsOfProject();
    }
}
