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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexEntity.Range;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.request.AggShardByColumnsRequest;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.IndexResponse;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("fusionIndexService")
public class FusionIndexService extends BasicService {
    private static final List<JobStatusEnum> runningStatus = Arrays.asList(JobStatusEnum.STARTING,
            JobStatusEnum.RUNNING, JobStatusEnum.STOPPING);

    @Autowired
    private IndexPlanService indexPlanService;

    @Transaction(project = 0)
    public Pair<IndexPlan, BuildIndexResponse> updateRuleBasedCuboid(String project,
            final UpdateRuleBasedCuboidRequest request) {
        val model = getDataModelManager(project).getDataModelDesc(request.getModelId());
        if (model.fusionModelStreamingPart()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            UpdateRuleBasedCuboidRequest batchCopy = JsonUtil.deepCopyQuietly(request,
                    UpdateRuleBasedCuboidRequest.class);
            UpdateRuleBasedCuboidRequest streamingCopy = JsonUtil.deepCopyQuietly(request,
                    UpdateRuleBasedCuboidRequest.class);
            batchCopy.setModelId(batchId);
            batchCopy.setAggregationGroups(getBatchAggGroup(request.getAggregationGroups()));
            streamingCopy.setAggregationGroups(getStreamingAggGroup(request.getAggregationGroups()));

            indexPlanService.updateRuleBasedCuboid(project, batchCopy);
            return indexPlanService.updateRuleBasedCuboid(project, streamingCopy);
        }
        return indexPlanService.updateRuleBasedCuboid(project, request);
    }

    public RuleBasedIndex getRule(String project, String modelId) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        val modelRule = indexPlanService.getRule(project, modelId);
        val newRuleBasedIndex = new RuleBasedIndex();
        if (!checkUpdateIndexEnabled(project, modelId)) {
            newRuleBasedIndex.setIndexUpdateEnabled(false);
        }

        if (modelRule != null) {
            newRuleBasedIndex.getAggregationGroups().addAll(modelRule.getAggregationGroups());
        }

        if (model.fusionModelStreamingPart()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(modelId);
            String batchId = fusionModel.getBatchModel().getUuid();
            val batchRule = indexPlanService.getRule(project, batchId);
            if (batchRule != null) {
                newRuleBasedIndex.getAggregationGroups().addAll(batchRule.getAggregationGroups().stream()
                        .filter(agg -> agg.getIndexRange() == IndexEntity.Range.BATCH).collect(Collectors.toList()));
            }
        }
        return newRuleBasedIndex;
    }

    @Transaction(project = 0)
    public BuildIndexResponse createTableIndex(String project, CreateTableIndexRequest request) {
        NDataModel model = getDataModelManager(project).getDataModelDesc(request.getModelId());
        checkStreamingIndexEnabled(project, model);

        if (model.fusionModelStreamingPart()) {
            if (!indexChangeEnable(project, request.getModelId(), request.getIndexRange(),
                    Lists.newArrayList(IndexEntity.Range.HYBRID, Range.STREAMING))) {
                throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getSTREAMING_INDEXES_ADD()));
            }
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            CreateTableIndexRequest copy = convertTableIndexRequest(request, model, batchId);
            if (IndexEntity.Range.BATCH == request.getIndexRange()) {
                return indexPlanService.createTableIndex(project, copy);
            } else if (IndexEntity.Range.HYBRID == request.getIndexRange()) {
                val indexPlanManager = getIndexPlanManager(project);
                val streamingIndexPlan = indexPlanManager.getIndexPlan(request.getModelId());
                val batchIndexPlan = indexPlanManager.getIndexPlan(batchId);
                long maxId = Math.max(streamingIndexPlan.getNextTableIndexId(), batchIndexPlan.getNextTableIndexId());
                indexPlanService.createTableIndex(project, copy, maxId + 1);
                return indexPlanService.createTableIndex(project, request, maxId + 1);
            }
        }
        return indexPlanService.createTableIndex(project, request);
    }

    private CreateTableIndexRequest convertTableIndexRequest(CreateTableIndexRequest request, NDataModel model,
            String batchId) {
        CreateTableIndexRequest copy = JsonUtil.deepCopyQuietly(request, CreateTableIndexRequest.class);
        copy.setModelId(batchId);
        String tableAlias = getDataModelManager(model.getProject()).getDataModelDesc(batchId).getRootFactTableRef()
                .getTableName();
        String oldAliasName = model.getRootFactTableRef().getTableName();
        convertTableIndex(copy, tableAlias, oldAliasName);
        return copy;
    }

    private void convertTableIndex(CreateTableIndexRequest copy, String tableName, String oldAliasName) {
        copy.setColOrder(copy.getColOrder().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setShardByColumns(copy.getShardByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setSortByColumns(copy.getSortByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
    }

    private String changeTableAlias(String col, String oldAlias, String newAlias) {
        String table = col.split("\\.")[0];
        String column = col.split("\\.")[1];
        if (table.equalsIgnoreCase(oldAlias)) {
            return newAlias + "." + column;
        }
        return col;
    }

    @Transaction(project = 0)
    public BuildIndexResponse updateTableIndex(String project, CreateTableIndexRequest request) {
        NDataModel model = getDataModelManager(project).getDataModelDesc(request.getModelId());
        checkStreamingIndexEnabled(project, model);

        if (model.fusionModelStreamingPart()) {
            if (!indexChangeEnable(project, request.getModelId(), request.getIndexRange(),
                    Lists.newArrayList(IndexEntity.Range.HYBRID, Range.STREAMING))) {
                throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getSTREAMING_INDEXES_EDIT()));
            }
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            CreateTableIndexRequest copy = convertTableIndexRequest(request, model, batchId);
            if (IndexEntity.Range.BATCH == request.getIndexRange()) {
                copy.setModelId(batchId);
                return indexPlanService.updateTableIndex(project, copy);
            } else if (IndexEntity.Range.HYBRID == request.getIndexRange()) {
                indexPlanService.updateTableIndex(project, copy);
            }
        }
        return indexPlanService.updateTableIndex(project, request);
    }

    public List<IndexResponse> getIndexes(String project, String modelId, String key, List<IndexEntity.Status> status,
            String orderBy, Boolean desc, List<IndexEntity.Source> sources, List<Long> ids,
            List<IndexEntity.Range> range) {
        List<IndexResponse> indexes = indexPlanService.getIndexes(project, modelId, key, status, orderBy, desc,
                sources);
        NDataModel model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.isFusionModel()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(modelId);
            if (fusionModel != null) {
                String batchId = fusionModel.getBatchModel().getUuid();
                String oldAliasName = getDataModelManager(project).getDataModelDesc(batchId).getRootFactTableRef()
                        .getTableName();
                String tableAlias = model.getRootFactTableRef().getTableName();
                indexes.addAll(indexPlanService.getIndexes(project, batchId, key, status, orderBy, desc, sources)
                        .stream().filter(index -> IndexEntity.Range.BATCH == index.getIndexRange())
                        .map(index -> convertTableIndex(index, tableAlias, oldAliasName)).collect(Collectors.toList()));
            } else {
                val streamingModel = getDataModelManager(project).getDataModelDesc(model.getFusionId());
                String oldAliasName = model.getRootFactTableRef().getTableName();
                String tableAlias = streamingModel.getRootFactTableRef().getTableName();
                indexes.stream().forEach(index -> convertTableIndex(index, tableAlias, oldAliasName));
            }
        }

        if (!CollectionUtils.isEmpty(ids)) {
            indexes = indexes.stream().filter(index -> (ids.contains(index.getId()))).collect(Collectors.toList());
        }

        if (!CollectionUtils.isEmpty(range)) {
            indexes = indexes.stream()
                    .filter(index -> (index.getIndexRange() == null || range.contains(index.getIndexRange())))
                    .collect(Collectors.toList());
        }
        return indexes;
    }

    private IndexResponse convertTableIndex(IndexResponse copy, String tableName, String oldAliasName) {
        copy.getColOrder().stream().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.setShardByColumns(copy.getShardByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setSortByColumns(copy.getSortByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        return copy;
    }

    @Transaction(project = 0)
    public void removeIndex(String project, String model, final long id, IndexEntity.Range indexRange) {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(model);
        checkStreamingIndexEnabled(project, modelDesc);

        if (modelDesc.fusionModelStreamingPart()) {
            if (!indexChangeEnable(project, model, indexRange,
                    Lists.newArrayList(IndexEntity.Range.HYBRID, Range.STREAMING, Range.EMPTY))) {
                throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getSTREAMING_INDEXES_DELETE()));
            }
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(model);
            String batchId = fusionModel.getBatchModel().getUuid();
            if (IndexEntity.Range.BATCH == indexRange) {
                indexPlanService.removeIndex(project, batchId, id);
                return;
            } else if (IndexEntity.Range.HYBRID == indexRange) {
                removeHybridIndex(project, batchId, id);
            }
        }
        indexPlanService.removeIndex(project, model, id);
    }

    @Transaction(project = 0)
    public void removeIndexes(String project, String modelId, Set<Long> ids) {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (modelDesc.isStreaming() && checkStreamingJobAndSegments(project, modelId)) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSTREAMING_INDEXES_DELETE()));

        }
        indexPlanService.removeIndexes(project, modelId, ids);
    }

    private void removeHybridIndex(String project, String model, final long id) {
        val indexPlan = getIndexPlanManager(project).getIndexPlan(model);
        if (indexPlan.getLayoutEntity(id) != null) {
            indexPlanService.removeIndex(project, model, id);
        }
    }

    public AggIndexResponse calculateAggIndexCount(UpdateRuleBasedCuboidRequest request) {
        if (isFusionModel(request.getProject(), request.getModelId())) {

            UpdateRuleBasedCuboidRequest batchRequest = convertBatchUpdateRuleReq(request);
            UpdateRuleBasedCuboidRequest streamRequest = convertStreamUpdateRuleReq(request);

            val batchResponse = batchRequest == null ? AggIndexResponse.empty()
                    : indexPlanService.calculateAggIndexCount(batchRequest);
            val streamResponse = streamRequest == null ? AggIndexResponse.empty()
                    : indexPlanService.calculateAggIndexCount(streamRequest);

            return AggIndexResponse.combine(batchResponse, streamResponse, request.getAggregationGroups().stream()
                    .map(NAggregationGroup::getIndexRange).collect(Collectors.toList()));
        }

        return indexPlanService.calculateAggIndexCount(request);
    }

    public DiffRuleBasedIndexResponse calculateDiffRuleBasedIndex(UpdateRuleBasedCuboidRequest request) {
        if (isFusionModel(request.getProject(), request.getModelId())) {
            UpdateRuleBasedCuboidRequest batchRequest = convertBatchUpdateRuleReq(request);
            UpdateRuleBasedCuboidRequest streamRequest = convertStreamUpdateRuleReq(request);

            val batchResponse = batchRequest == null ? DiffRuleBasedIndexResponse.empty()
                    : indexPlanService.calculateDiffRuleBasedIndex(batchRequest);
            val streamResponse = streamRequest == null ? DiffRuleBasedIndexResponse.empty()
                    : indexPlanService.calculateDiffRuleBasedIndex(streamRequest);
            checkStreamingAggEnabled(streamResponse, request.getProject(), request.getModelId());
            return DiffRuleBasedIndexResponse.combine(batchResponse, streamResponse);
        }

        DiffRuleBasedIndexResponse response = indexPlanService.calculateDiffRuleBasedIndex(request);

        val model = getDataModelManager(request.getProject()).getDataModelDesc(request.getModelId());
        if (NDataModel.ModelType.STREAMING == model.getModelType()) {
            checkStreamingAggEnabled(response, request.getProject(), request.getModelId());
        }

        return response;
    }

    private UpdateRuleBasedCuboidRequest convertStreamUpdateRuleReq(UpdateRuleBasedCuboidRequest request) {
        UpdateRuleBasedCuboidRequest streamRequest = JsonUtil.deepCopyQuietly(request,
                UpdateRuleBasedCuboidRequest.class);
        streamRequest.setAggregationGroups(getStreamingAggGroup(streamRequest.getAggregationGroups()));
        if (CollectionUtils.isEmpty(streamRequest.getAggregationGroups())) {
            return null;
        }
        return streamRequest;
    }

    private UpdateRuleBasedCuboidRequest convertBatchUpdateRuleReq(UpdateRuleBasedCuboidRequest request) {
        val batchRequest = JsonUtil.deepCopyQuietly(request, UpdateRuleBasedCuboidRequest.class);
        batchRequest.setAggregationGroups(getBatchAggGroup(batchRequest.getAggregationGroups()));
        if (CollectionUtils.isEmpty(batchRequest.getAggregationGroups())) {
            return null;
        }
        FusionModel fusionModel = getFusionModelManager(request.getProject()).getFusionModel(request.getModelId());
        batchRequest.setModelId(fusionModel.getBatchModel().getUuid());
        return batchRequest;
    }

    private boolean isFusionModel(String project, String modelId) {
        return getDataModelManager(project).getDataModelDesc(modelId).fusionModelStreamingPart();
    }

    private List<NAggregationGroup> getStreamingAggGroup(List<NAggregationGroup> aggregationGroups) {
        return aggregationGroups.stream().filter(agg -> (agg.getIndexRange() == IndexEntity.Range.STREAMING
                || agg.getIndexRange() == IndexEntity.Range.HYBRID)).collect(Collectors.toList());
    }

    private List<NAggregationGroup> getBatchAggGroup(List<NAggregationGroup> aggregationGroups) {
        return aggregationGroups.stream()
                .filter(agg -> (agg.getIndexRange() == Range.BATCH || agg.getIndexRange() == IndexEntity.Range.HYBRID))
                .collect(Collectors.toList());
    }

    @Transaction(project = 0)
    public void updateShardByColumns(String project, AggShardByColumnsRequest aggShardByColumnsRequest) {
        if (isFusionModel(project, aggShardByColumnsRequest.getModelId())) {
            val batchId = getBatchModel(project, aggShardByColumnsRequest.getModelId());
            val batchRequest = JsonUtil.deepCopyQuietly(aggShardByColumnsRequest, AggShardByColumnsRequest.class);
            batchRequest.setModelId(batchId);
            indexPlanService.updateShardByColumns(project, batchRequest);
        }
        indexPlanService.updateShardByColumns(project, aggShardByColumnsRequest);
    }

    public List<IndexResponse> getAllIndexes(String project, String modelId, String key,
            List<IndexEntity.Status> status, String orderBy, Boolean desc, List<IndexEntity.Source> sources) {
        if (isFusionModel(project, modelId)) {
            val batchId = getBatchModel(project, modelId);
            List<IndexResponse> response = new ArrayList<>();
            List<IndexResponse> batchResponse = indexPlanService.getIndexes(project, batchId, key, status, orderBy,
                    desc, sources);
            batchResponse.stream().forEach(index -> index.setIndexRange(Range.BATCH));
            response.addAll(batchResponse);

            List<IndexResponse> streamingResponse = indexPlanService.getIndexes(project, modelId, key, status, orderBy,
                    desc, sources);
            streamingResponse.stream().forEach(index -> index.setIndexRange(Range.STREAMING));
            response.addAll(streamingResponse);
            return response;
        }
        return indexPlanService.getIndexes(project, modelId, key, status, orderBy, desc, sources);
    }

    public List<IndexResponse> getIndexesWithRelatedTables(String project, String modelId, String key,
            List<IndexEntity.Status> status, String orderBy, Boolean desc, List<IndexEntity.Source> sources,
            List<Long> batchIndexIds) {
        if (isFusionModel(project, modelId)) {
            val batchId = getBatchModel(project, modelId);
            List<IndexResponse> response = new ArrayList<>();
            List<IndexResponse> batchResponse = indexPlanService.getIndexesWithRelatedTables(project, batchId, key,
                    status, orderBy, desc, sources, batchIndexIds);
            batchResponse.forEach(index -> index.setIndexRange(Range.BATCH));
            response.addAll(batchResponse);

            List<IndexResponse> streamingResponse = indexPlanService.getIndexes(project, modelId, key, status, orderBy,
                    desc, sources);
            streamingResponse.forEach(index -> index.setIndexRange(Range.STREAMING));
            response.addAll(streamingResponse);
            return response;
        }
        return indexPlanService.getIndexesWithRelatedTables(project, modelId, key, status, orderBy, desc, sources, batchIndexIds);
    }

    private String getBatchModel(String project, String modelId) {
        FusionModel fusionModel = getFusionModelManager(project).getFusionModel(modelId);
        return fusionModel.getBatchModel().getId();
    }

    private static void checkStreamingAggEnabled(DiffRuleBasedIndexResponse streamResponse, String project,
            String modelId) {
        if ((streamResponse.getDecreaseLayouts() > 0 || streamResponse.getIncreaseLayouts() > 0)
                && checkStreamingJobAndSegments(project, modelId)) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE, MsgPicker.getMsg().getSTREAMING_INDEXES_EDIT());
        }
    }

    public static boolean checkUpdateIndexEnabled(String project, String modelId) {
        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataModelDesc(modelId);
        if (model == null) {
            log.warn("model {} is not existed in project:{}", modelId, project);
            return false;
        }
        return (NDataModel.ModelType.STREAMING != model.getModelType()
                && NDataModel.ModelType.HYBRID != model.getModelType())
                || !checkStreamingJobAndSegments(project, model.getUuid());
    }

    private static void checkStreamingIndexEnabled(String project, NDataModel model) throws KylinException {
        if (NDataModel.ModelType.STREAMING == model.getModelType()
                && checkStreamingJobAndSegments(project, model.getUuid())) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE, MsgPicker.getMsg().getSTREAMING_INDEXES_DELETE());
        }
    }

    private static boolean indexChangeEnable(String project, String modelId, IndexEntity.Range range,
            List<IndexEntity.Range> ranges) {
        if (!ranges.contains(range)) {
            return true;
        }
        return !checkStreamingJobAndSegments(project, modelId);
    }

    public static boolean checkStreamingJobAndSegments(String project, String modelId) {
        String jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        val config = KylinConfig.getInstanceFromEnv();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        StreamingJobMeta meta = mgr.getStreamingJobByUuid(jobId);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        NDataflow df = dataflowManager.getDataflow(modelId);
        return runningStatus.contains(meta.getCurrentStatus()) || !df.getSegments().isEmpty();
    }
}
