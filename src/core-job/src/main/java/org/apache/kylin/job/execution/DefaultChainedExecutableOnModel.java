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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import lombok.val;

public class DefaultChainedExecutableOnModel extends DefaultChainedExecutable {

    private String getTargetModel() {
        return getTargetSubject();
    }

    @Override
    public void onExecuteErrorHook(String jobId) {
        markDFLagBehindIfNecessary(jobId);
    }

    private void markDFLagBehindIfNecessary(String jobId) {
        if (!JobTypeEnum.INC_BUILD.equals(this.getJobType())) {
            return;
        }
        val dataflow = getDataflow(jobId);
        if (dataflow == null || RealizationStatusEnum.LAG_BEHIND.equals(dataflow.getStatus())) {
            return;
        }
        val model = dataflow.getModel();
        if (ManagementType.MODEL_BASED.equals(model.getManagementType())) {
            return;
        }

        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        dfManager.updateDataflow(dataflow.getId(),
                copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND));
    }

    private NDataflow getDataflow(String jobId) {
        val execManager = getExecutableManager(getProject());
        val executable = (DefaultChainedExecutableOnModel) execManager.getJob(jobId);
        val modelId = executable.getTargetModel();
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        return dfManager.getDataflow(modelId);
    }

    @Override
    public boolean checkSuicide() {
        try {
            return !checkAnyTargetSegmentExists() || checkCuttingInJobByModel() || !checkAnyLayoutExists();
        } catch (Exception e) {
            return true;
        }
    }

    private boolean checkAnyLayoutExists() {
        String layouts = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (StringUtils.isEmpty(layouts)) {
            return true;
        }
        val cubeManager = NIndexPlanManager.getInstance(getConfig(), getProject());
        val cube = cubeManager.getIndexPlan(getTargetModel());
        val allLayoutIds = cube.getAllLayouts().stream().map(l -> l.getId() + "").collect(Collectors.toSet());
        return Stream.of(StringUtil.splitAndTrim(layouts, ",")).anyMatch(allLayoutIds::contains);
    }

    private boolean checkTargetSegmentExists(String segmentId) {
        NDataflow dataflow = NDataflowManager.getInstance(getConfig(), getProject()).getDataflow(getTargetModel());
        if (dataflow == null) {
            return false;
        }
        NDataSegment segment = dataflow.getSegment(segmentId);
        return segment != null;
    }

    public boolean checkAnyTargetSegmentExists() {
        List<String> topJobTargetSegments = getTargetSegments();
        AbstractExecutable parent = getParent();
        if (parent != null) {
            topJobTargetSegments = parent.getTargetSegments();
        }

        Preconditions.checkState(!topJobTargetSegments.isEmpty());

        return topJobTargetSegments.stream().anyMatch(this::checkTargetSegmentExists);
    }

    public boolean checkCuttingInJobByModel() {
        AbstractExecutable parent = getParent();
        if (parent == null) {
            parent = this;
        }
        if (!JobTypeEnum.INDEX_BUILD.equals(parent.getJobType())) {
            return false;
        }
        val model = ((DefaultChainedExecutableOnModel) parent).getTargetModel();
        return NExecutableManager.getInstance(getConfig(), getProject()).countCuttingInJobByModel(model, parent) > 0;
    }

}
