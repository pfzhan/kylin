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

package org.apache.kylin.job.handler;

import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactoryConstant;
import org.apache.kylin.job.model.JobParam;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;

public class SecondStorageSegmentCleanJobHandler extends AbstractSecondStorageJobHanlder {
    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, jobParam.getProject())
                .getDataflow(jobParam.getModel());
        SecondStorageCleanJobBuildParams params = new SecondStorageCleanJobBuildParams(
                jobParam.getTargetSegments().stream().map(dataflow::getSegment).collect(Collectors.toSet()),
                jobParam,
                JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN);
        params.setProject(dataflow.getProject());
        params.setModelId(dataflow.getModel().getId());
        params.setDataflowId(dataflow.getId());
        return JobFactory.createJob(JobFactoryConstant.STORAGE_SEGMENT_CLEAN_FACTORY, params);
    }
}
