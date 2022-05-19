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

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.job.execution.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.rest.delegate.ModelMetadataBaseInvoker;
import lombok.val;

public class ExecutableAddCuboidHandler extends ExecutableHandler {
    private static final Logger logger = LoggerFactory.getLogger(ExecutableAddCuboidHandler.class);

    public ExecutableAddCuboidHandler(DefaultChainedExecutableOnModel job) {
        this(job.getProject(), job.getTargetSubject(), job.getSubmitter(), null, job.getId());
    }

    public ExecutableAddCuboidHandler(String project, String modelId, String owner, String segmentId, String jobId) {
        super(project, modelId, owner, segmentId, jobId);
    }

    @Override
    public void handleFinished() {
        val project = getProject();
        val jobId = getJobId();
        val modelId = getModelId();
        val executable = getExecutable();
        Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val merger = new AfterBuildResourceMerger(kylinConfig, project);
        executable.getTasks().stream() //
                .filter(task -> task instanceof NSparkExecutable) //
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata())
                .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));

        Optional.ofNullable(executable.getParams()).ifPresent(params -> {
            String toBeDeletedLayoutIdsStr = params.get(NBatchConstants.P_TO_BE_DELETED_LAYOUT_IDS);
            if (StringUtils.isNotBlank(toBeDeletedLayoutIdsStr)) {
                logger.info("Try to delete the toBeDeletedLayoutIdsStr: {}, jobId: {}", toBeDeletedLayoutIdsStr, jobId);
                Set<Long> toBeDeletedLayoutIds = new LinkedHashSet<>();
                for (String id : toBeDeletedLayoutIdsStr.split(",")) {
                    toBeDeletedLayoutIds.add(Long.parseLong(id));
                }
                ModelMetadataBaseInvoker.getInstance().updateIndex(project, -1, modelId, toBeDeletedLayoutIds,
                        true, true);
            }
        });
        markDFStatus();
    }

    @Override
    public void handleDiscardOrSuicidal() {
        val job = getExecutable();
        // anyTargetSegmentExists && checkCuttingInJobByModel need restart job
        if (!(job.checkCuttingInJobByModel() && job.checkAnyTargetSegmentAndPartitionExists())) {
            return;
        }
    }

}
