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

import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;
import io.kyligence.kap.streaming.event.StreamingJobKillEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("fusionModelService")
public class FusionModelService extends BasicService implements TableFusionModelSupporter {

    @Autowired
    private ModelService modelService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildSupporter modelBuildService;

    public JobInfoResponse incrementBuildSegmentsManually(IncrementBuildSegmentParams params) throws Exception {
        val model = getDataModelManager(params.getProject()).getDataModelDesc(params.getModelId());
        if (model.isFusionModel()) {
            val streamingModel = getDataModelManager(params.getProject()).getDataModelDesc(model.getFusionId());
            IncrementBuildSegmentParams copy = JsonUtil.deepCopyQuietly(params, IncrementBuildSegmentParams.class);
            String oldAliasName = streamingModel.getRootFactTableRef().getTableName();
            String tableName = model.getRootFactTableRef().getTableName();
            copy.getPartitionDesc().changeTableAlias(oldAliasName, tableName);
            return modelBuildService.incrementBuildSegmentsManually(copy);
        }
        return modelBuildService.incrementBuildSegmentsManually(params);
    }

    @Transaction(project = 1)
    public void dropModel(String modelId, String project) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.fusionModelStreamingPart()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val fusionModel = fusionModelManager.getFusionModel(modelId);
            String batchId = fusionModel.getBatchModel().getUuid();
            fusionModelManager.dropModel(modelId);
            modelService.dropModel(batchId, project);
        }
        modelService.dropModel(modelId, project);
    }

    public void dropModel(String modelId, String project, boolean ignoreType) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model == null) {
            return;
        }

        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val fusionModel = fusionModelManager.getFusionModel(modelId);
            if (model.fusionModelBatchPart()) {
                String streamingId = model.getFusionId();
                fusionModelManager.dropModel(streamingId);
                modelService.dropModel(streamingId, project, ignoreType);
            } else {
                String batchId = fusionModel.getBatchModel().getUuid();
                fusionModelManager.dropModel(modelId);
                modelService.dropModel(batchId, project, ignoreType);
            }
        }
        modelService.dropModel(modelId, project, ignoreType);
    }

    @Transaction(project = 0)
    public BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request) {
        val model = getDataModelManager(request.getProject()).getDataModelDesc(request.getUuid());
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            String batchId = fusionModelManager.getFusionModel(request.getUuid()).getBatchModel().getUuid();
            ModelRequest copy = JsonUtil.deepCopyQuietly(request, ModelRequest.class);
            String tableName = model.getRootFactTableRef().getTableDesc().getKafkaConfig().getBatchTable();

            copy.setAlias(FusionModel.getBatchName(model.getAlias(), model.getUuid()));
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
            modelService.renameDataModel(project, batchId, FusionModel.getBatchName(newAlias, modelId));
        }
        modelService.renameDataModel(project, modelId, newAlias);
        if (model.isStreaming() || model.isFusionModel()) {
            // Sync update of streaming job meta model name
            EventBusFactory.getInstance().postSync(new NDataModel.ModelRenameEvent(project, modelId, newAlias));
        }
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

    public Pair<String, String[]> convertSegmentIdWithName(String modelId, String project, String[] segIds,
            String[] segNames) {
        if (ArrayUtils.isEmpty(segNames)) {
            return new Pair<>(modelId, segIds);
        }
        val dataModel = modelService.getModelById(modelId, project);
        String targetModelId = modelId;
        if (dataModel.isFusionModel()) {
            boolean existedInStreaming = modelService.checkSegmentsExistByName(targetModelId, project, segNames, false);
            if (!existedInStreaming) {
                targetModelId = getBatchModel(modelId, project).getUuid();
            }
        }
        String[] segmentIds = modelService.convertSegmentIdWithName(targetModelId, project, segIds, segNames);
        return new Pair<>(targetModelId, segmentIds);
    }

    public JobInfoResponseWithFailure addIndexesToSegments(String modelId,
            IndexesToSegmentsRequest buildSegmentsRequest) {
        String targetModelId = modelId;
        NDataModel dataModel = modelService.getModelById(modelId, buildSegmentsRequest.getProject());
        if (dataModel.getModelType() == NDataModel.ModelType.HYBRID) {
            boolean existedInStreaming = modelService.checkSegmentsExistById(targetModelId,
                    buildSegmentsRequest.getProject(), buildSegmentsRequest.getSegmentIds().toArray(new String[0]),
                    false);
            if (existedInStreaming) {
                throw new KylinException(ServerErrorCode.SEGMENT_UNSUPPORTED_OPERATOR,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getFIX_STREAMING_SEGMENT()));
            } else {
                targetModelId = getBatchModel(modelId, buildSegmentsRequest.getProject()).getUuid();
            }
        } else if (dataModel.getModelType() == NDataModel.ModelType.STREAMING) {
            throw new KylinException(ServerErrorCode.SEGMENT_UNSUPPORTED_OPERATOR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getFIX_STREAMING_SEGMENT()));
        }

        return modelBuildService.addIndexesToSegments(buildSegmentsRequest.getProject(),
                targetModelId, buildSegmentsRequest.getSegmentIds(), buildSegmentsRequest.getIndexIds(),
                buildSegmentsRequest.isParallelBuildBySegment(), buildSegmentsRequest.getPriority(),
                buildSegmentsRequest.isPartialBuild(), buildSegmentsRequest.getYarnQueue(),
                buildSegmentsRequest.getTag());
    }

    private NDataModel getBatchModel(String fusionModelId, String project) {
        val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val fusionModel = fusionModelManager.getFusionModel(fusionModelId);
        return fusionModel.getBatchModel();
    }

    public void stopStreamingJob(String modelId, String project) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.fusionModelBatchPart()) {
            val streamingId = model.getFusionId();
            EventBusFactory.getInstance().postSync(new StreamingJobKillEvent(project, streamingId));
        }
        if (model.isStreaming()) {
            EventBusFactory.getInstance().postSync(new StreamingJobKillEvent(project, modelId));
        }
    }

    @Override
    public void onDropModel(String modelId, String project, boolean ignoreType) {
        dropModel(modelId, project, true);
    }

    @Override
    public void onStopStreamingJob(String modelId, String project) {
        stopStreamingJob(modelId, project);
    }

    public void setModelUpdateEnabled(DataResult<List<NDataModel>> dataResult) {
        val dataModelList = dataResult.getValue();
        dataModelList.stream().filter(model -> model.isStreaming()).forEach(model -> {
            if (model.isBroken()) {
                ((NDataModelResponse) model).setModelUpdateEnabled(false);
            } else {
                ((NDataModelResponse) model).setModelUpdateEnabled(
                        !FusionIndexService.checkStreamingJobAndSegments(model.getProject(), model.getUuid()));
            }
        });
    }
    public boolean modelExists(String modelAlias, String project) {
        return getDataModelManager(project).listAllModelAlias().contains(modelAlias);
    }
}
