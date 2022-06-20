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

package io.kyligence.kap.job.execution.handler;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.manager.JobManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.rest.delegate.ModelMetadataBaseInvoker;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public abstract class ExecutableHandler {

    protected static final String SUBJECT_NOT_EXIST_COMMENT = "subject does not exist or is broken, roll back to to-be-accelerated status";

    @Getter
    @Setter
    private String project;
    @Getter
    @Setter
    private String modelId;
    @Getter
    @Setter
    private String owner;
    @Getter
    @Setter
    private String segmentId;
    @Getter
    @Setter
    private String jobId;

    public abstract void handleFinished();

    public abstract void handleDiscardOrSuicidal();

    protected ExecutableManager getExecutableManager(String project, KylinConfig config) {
        return ExecutableManager.getInstance(config, project);
    }

    protected void addJob(String segmentId, JobTypeEnum jobTypeEnum) {
        JobManager manager = JobManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataSegment segment = NDataSegment.empty();
        if (segmentId != null) {
            segment.setId(segmentId);
        }
        manager.addJob(new JobParam(segment, modelId, owner).withJobTypeEnum(jobTypeEnum));
    }

    protected DefaultChainedExecutableOnModel getExecutable() {
        val executable = getExecutableManager(project, KylinConfig.getInstanceFromEnv()).getJob(jobId);
        Preconditions.checkNotNull(executable);
        Preconditions.checkArgument(executable instanceof DefaultChainedExecutableOnModel);
        return (DefaultChainedExecutableOnModel) executable;
    }

    public void markDFStatus() {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow df = dfManager.getDataflow(getModelId());
        boolean isOffline = dfManager.isOfflineModel(df);
        RealizationStatusEnum status = df.getStatus();
        if (RealizationStatusEnum.ONLINE == status && isOffline) {
            dfManager.updateDataflowStatus(df.getId(), RealizationStatusEnum.OFFLINE);
        } else if (RealizationStatusEnum.OFFLINE == status && !isOffline) {
            ModelMetadataBaseInvoker.getInstance().updateDataflowStatus(project, df.getId(), RealizationStatusEnum.ONLINE);
        }
    }
}
