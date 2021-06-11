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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("fusionModelService")
public class FusionModelService extends BasicService {

    @Autowired
    private ModelService modelService;

    public JobInfoResponse incrementBuildSegmentsManually(IncrementBuildSegmentParams params) throws Exception {
        val model = getDataModelManager(params.getProject()).getDataModelDesc(params.getModelId());
        if (model.isFusionModel()) {
            val streamingModel = getDataModelManager(params.getProject()).getDataModelDesc(model.getFusionId());
            IncrementBuildSegmentParams copy = JsonUtil.deepCopyQuietly(params, IncrementBuildSegmentParams.class);
            String oldAliasName = streamingModel.getRootFactTableRef().getTableName();
            String tableName = model.getRootFactTableRef().getTableName();
            copy.getPartitionDesc().changeTableAlias(oldAliasName, tableName);
            return modelService.incrementBuildSegmentsManually(copy);
        }
        return modelService.incrementBuildSegmentsManually(params);
    }

    @Transaction(project = 1)
    public void dropModel(String modelId, String project) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val fusionModel = fusionModelManager.getFusionModel(modelId);
            String batchId = fusionModel.getBatchModel().getUuid();
            fusionModelManager.dropModel(modelId);
            modelService.dropModel(batchId, project);
        }
        modelService.dropModel(modelId, project);
    }

    @Transaction(project = 0)
    public BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request) {
        val model = getDataModelManager(request.getProject()).getDataModelDesc(request.getUuid());
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            String batchId = fusionModelManager.getFusionModel(request.getUuid()).getBatchModel().getUuid();
            ModelRequest copy = JsonUtil.deepCopyQuietly(request, ModelRequest.class);
            String tableName = model.getRootFactTableRef().getTableDesc().getKafkaConfig().getBatchTable();

            copy.setAlias(model.getAlias() + "_batch");
            copy.setRootFactTableName(tableName);
            copy.setUuid(batchId);

            String tableAlias = model.getRootFactTableRef().getTableDesc().getKafkaConfig().getBatchTableAlias();
            String oldAliasName = model.getRootFactTableRef().getTableName();
            convertModel(copy, tableAlias, oldAliasName);
            modelService.updateDataModelSemantic(project, copy);
        }
        return modelService.updateDataModelSemantic(project, request);
    }

    private void convertModel(ModelRequest copy, String tableName, String oldAliasName) {
        copy.getSimplifiedJoinTableDescs().stream()
                .forEach(x -> x.getSimplifiedJoinDesc().changeFKTableAlias(oldAliasName, tableName));
        copy.getSimplifiedDimensions().stream().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.getSimplifiedMeasures().stream().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.getPartitionDesc().changeTableAlias(oldAliasName, tableName);
    }

    @Transaction(project = 0)
    public void renameDataModel(String project, String modelId, String newAlias) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            String batchId = fusionModelManager.getFusionModel(modelId).getBatchModel().getUuid();
            modelService.renameDataModel(project, batchId, FusionModel.getBatchName(newAlias));
        }
        modelService.renameDataModel(project, modelId, newAlias);
    }

    @Transaction(project = 0)
    public void updateModelOwner(String project, String modelId, OwnerChangeRequest ownerChangeRequest) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            String batchId = fusionModelManager.getFusionModel(modelId).getBatchModel().getUuid();
            OwnerChangeRequest batchRequest = JsonUtil.deepCopyQuietly(ownerChangeRequest, OwnerChangeRequest.class);
            modelService.updateModelOwner(project, batchId, batchRequest);
        }
        modelService.updateModelOwner(project, modelId, ownerChangeRequest);
    }

}
