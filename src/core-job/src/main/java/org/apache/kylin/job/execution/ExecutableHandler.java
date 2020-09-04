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

package org.apache.kylin.job.execution;

import static org.apache.kylin.metadata.realization.RealizationStatusEnum.OFFLINE;
import static org.apache.kylin.metadata.realization.RealizationStatusEnum.ONLINE;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public abstract class ExecutableHandler implements IKeepNames {

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

    protected NExecutableManager getExecutableManager(String project, KylinConfig config) {
        return NExecutableManager.getInstance(config, project);
    }

    protected void addJob(String segmentId, JobTypeEnum jobTypeEnum) {
        JobManager manager = JobManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataSegment segment = new NDataSegment();
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
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val prjManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(getProject());
        val df = dfManager.getDataflow(getModelId());
        boolean offlineManually = df.getIndexPlan().isOfflineManually();
        boolean offlineScd = SCD2CondChecker.INSTANCE.isScd2Model(df.getModel()) && !prjManager.getConfig().isQueryNonEquiJoinModelEnabled();
        val status = df.getStatus();
        switch (status) {
            case ONLINE:
                if (offlineManually || offlineScd) {
                    dfManager.updateDataflow(df.getId(),
                            copyForWrite -> copyForWrite.setStatus(OFFLINE));
                }
                break;
            case OFFLINE:
                if (!offlineManually && !offlineScd) {
                    dfManager.updateDataflow(df.getId(),
                            copyForWrite -> copyForWrite.setStatus(ONLINE));
                }
                break;
            default:
                break;
        }
    }
}
