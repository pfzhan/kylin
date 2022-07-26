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
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;

public interface JobMetadataContract {

    String addIndexJob(JobMetadataRequest jobMetadataRequest);

    String addSecondStorageJob(JobMetadataRequest jobMetadataRequest);

    String addSegmentJob(JobMetadataRequest jobMetadataRequest);

    Set<Long> getLayoutsByRunningJobs(String project, String modelId);

    long countByModelAndStatus(String project, String model, String status, JobTypeEnum... jobTypes);

    List<ExecutablePO> getJobExecutablesPO(String project);

    List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state, JobTypeEnum... jobTypes);

    List<ExecutablePO> getExecutablePOsByStatus(String project, ExecutableState... status);

    void deleteJobByIdList(String project, List<String> jobIdList);

    void discardJob(String project, String jobId);

    void stopBatchJob(String project, TableDesc tableDesc);

    void clearJobsByProject(String project);

}
