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

package io.kyligence.kap.job.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.rest.JobMapperFilter;

@Mapper
public interface JobInfoMapper {
    int deleteByPrimaryKey(Long id);

    int deleteByProject(String project);

    int insert(JobInfo row);

    int insertSelective(JobInfo row);

    JobInfo selectByPrimaryKey(Long id);

    int updateByPrimaryKeySelective(JobInfo row);

    int updateByPrimaryKeyWithBLOBs(JobInfo row);

    int updateByPrimaryKey(JobInfo row);

    // ----------------------------------------

    int deleteByJobId(@Param("jobId") String jobId);

    int deleteByJobIdList(@Param("jobStatusList") List<String> jobStatusList, @Param("jobIdList") List<String> jobIdList);

    int deleteAllJob();

    int insertJobInfoSelective(JobInfo jobInfo);

    JobInfo selectByJobId(@Param("jobId") String jobId);

    int updateByJobIdSelective(JobInfo jobInfo);

    List<String> findJobIdListByStatusBatch(@Param("status") String status, @Param("batchSize") int batchSize);

    int updateJobStatusByJobId(@Param("jobId") String jobId, @Param("status") String status);

    List<JobInfo> selectByJobFilter(JobMapperFilter jobMapperFilter);
}