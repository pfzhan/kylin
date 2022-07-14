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

package io.kyligence.kap.rest.service;

import java.util.Set;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.rest.request.DataFlowUpdateRequest;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_ID_NOT_EXIST;

public class ModelMetadataBaseService {

    public String getModelNameById(String modelId, String project) {
        NDataModel nDataModel = getModelById(modelId, project);
        if (null != nDataModel) {
            return nDataModel.getAlias();
        }
        return null;
    }

    private NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new KylinException(MODEL_ID_NOT_EXIST, modelId);
        }
        return nDataModel;
    }

    public void updateIndex(String project, long epochId, String modelId, Set<Long> toBeDeletedLayoutIds,
                            boolean deleteAuto, boolean deleteManual) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(modelId,
                    copyForWrite -> copyForWrite.removeLayouts(toBeDeletedLayoutIds, deleteAuto, deleteManual));
            return null;
        }, epochId, project);
    }

    public void updateDataflow(DataFlowUpdateRequest dataFlowUpdateRequest) {
        String project = dataFlowUpdateRequest.getProject();
        NDataflowUpdate update = dataFlowUpdateRequest.getDataflowUpdate();

        // After serialization and deserialization， layout will lost dataSegDetails, try to restore it here.
        NDataSegDetails[] dataSegDetails = dataFlowUpdateRequest.getDataSegDetails();
        Integer[] layoutCounts = dataFlowUpdateRequest.getLayoutCounts();
        if (dataSegDetails != null && layoutCounts != null) {
            int layoutCount = 0;
            for (int i = 0; i < dataSegDetails.length; i++) {
                for (int j = layoutCount; j < layoutCount + layoutCounts[i]; j++) {
                    update.getToAddOrUpdateLayouts()[j].setSegDetails(dataSegDetails[i]);
                }
                layoutCount += layoutCounts[i];
            }
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(update);
            return null;
        }, project);
    }

    public void updateDataflow(String project, String dfId, String segmentId, long maxBucketId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(dfId,
                    copyForWrite -> copyForWrite.getSegment(segmentId).setMaxBucketId(maxBucketId));
            return null;
        }, project);
    }

    public void updateIndexPlan(String project, String uuid, IndexPlan indexplan, String action) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            if ("setLayoutBucketNumMapping".equals(action)) {
                NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(uuid,
                        copyForWrite -> copyForWrite.setLayoutBucketNumMapping(indexplan.getLayoutBucketNumMapping()));
            } else if ("removeTobeDeleteIndexIfNecessary".equals(action)) {
                NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(uuid,
                        IndexPlan::removeTobeDeleteIndexIfNecessary);
            } else {
                throw new IllegalStateException(String.format("action {%s} is nor illegal !!", action));
            }
            return null;
        }, project);
    }

    public void updateDataflowStatus(String project, String uuid, RealizationStatusEnum status) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflowStatus(uuid, status);
            return null;
        }, project);
    }
}