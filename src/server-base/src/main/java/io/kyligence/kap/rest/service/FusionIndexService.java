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

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.IndexResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service("fusionIndexService")
public class FusionIndexService extends BasicService {

    @Autowired
    private IndexPlanService indexPlanService;

    @Transaction(project = 0)
    public Pair<IndexPlan, BuildIndexResponse> updateRuleBasedCuboid(String project,
            final UpdateRuleBasedCuboidRequest request) {
        val model = getDataModelManager(project).getDataModelDesc(request.getModelId());
        if (model.showFusionModel()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            UpdateRuleBasedCuboidRequest batchCopy = JsonUtil.deepCopyQuietly(request,
                    UpdateRuleBasedCuboidRequest.class);
            UpdateRuleBasedCuboidRequest streamingCopy = JsonUtil.deepCopyQuietly(request,
                    UpdateRuleBasedCuboidRequest.class);
            batchCopy.setModelId(batchId);
            batchCopy
                    .setAggregationGroups(
                            request.getAggregationGroups().stream()
                                    .filter(agg -> (agg.getIndexRange() == IndexEntity.Range.BATCH
                                            || agg.getIndexRange() == IndexEntity.Range.HYBRID))
                                    .collect(Collectors.toList()));
            streamingCopy
                    .setAggregationGroups(
                            request.getAggregationGroups().stream()
                                    .filter(agg -> (agg.getIndexRange() == IndexEntity.Range.STREAMING
                                            || agg.getIndexRange() == IndexEntity.Range.HYBRID))
                                    .collect(Collectors.toList()));

            indexPlanService.updateRuleBasedCuboid(project, batchCopy);
            return indexPlanService.updateRuleBasedCuboid(project, streamingCopy);
        }
        return indexPlanService.updateRuleBasedCuboid(project, request);
    }

    public RuleBasedIndex getRule(String project, String modelId) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.showFusionModel()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(modelId);
            String batchId = fusionModel.getBatchModel().getUuid();
            RuleBasedIndex newRuleBasedIndex = new RuleBasedIndex();
            RuleBasedIndex streamingRule = indexPlanService.getRule(project, modelId);
            RuleBasedIndex batchRule = indexPlanService.getRule(project, batchId);
            if (streamingRule != null) {
                newRuleBasedIndex.getAggregationGroups().addAll(streamingRule.getAggregationGroups());
            }
            if (batchRule != null) {
                newRuleBasedIndex.getAggregationGroups().addAll(batchRule.getAggregationGroups().stream()
                        .filter(agg -> agg.getIndexRange() == IndexEntity.Range.BATCH).collect(Collectors.toList()));
            }

            return newRuleBasedIndex;
        }
        return indexPlanService.getRule(project, modelId);
    }

    @Transaction(project = 0)
    public BuildIndexResponse createTableIndex(String project, CreateTableIndexRequest request) {
        NDataModel model = getDataModelManager(project).getDataModelDesc(request.getModelId());
        if (model.showFusionModel()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            CreateTableIndexRequest copy = JsonUtil.deepCopyQuietly(request, CreateTableIndexRequest.class);
            copy.setModelId(batchId);
            String tableAlias = getDataModelManager(project).getDataModelDesc(batchId).getRootFactTableRef()
                    .getTableName();
            String oldAliasName = model.getRootFactTableRef().getTableName();
            convertTableIndex(copy, tableAlias, oldAliasName);
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

    private void convertTableIndex(CreateTableIndexRequest copy, String tableName, String oldAliasName) {
        copy.setColOrder(copy.getColOrder().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setShardByColumns(copy.getShardByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setSortByColumns(copy.getSortByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
    }

    public String changeTableAlias(String col, String oldAlias, String newAlias) {
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
        if (model.showFusionModel()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            CreateTableIndexRequest copy = JsonUtil.deepCopyQuietly(request, CreateTableIndexRequest.class);
            copy.setModelId(batchId);
            copy.setModelId(batchId);
            String tableAlias = getDataModelManager(project).getDataModelDesc(batchId).getRootFactTableRef()
                    .getTableName();
            String oldAliasName = model.getRootFactTableRef().getTableName();
            convertTableIndex(copy, tableAlias, oldAliasName);
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
            String orderBy, Boolean desc, List<IndexEntity.Source> sources, List<Long> ids) {
        List<IndexResponse> indexes = indexPlanService.getIndexes(project, modelId, key, status, orderBy, desc,
                sources);
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (modelDesc.showFusionModel()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(modelId);
            String batchId = fusionModel.getBatchModel().getUuid();
            indexes.addAll(indexPlanService.getIndexes(project, batchId, key, status, orderBy, desc, sources).stream()
                    .filter(index -> IndexEntity.Range.BATCH == index.getIndexRange()).collect(Collectors.toList()));
        }
        if (CollectionUtils.isEmpty(ids)) {
            return indexes;
        }
        return indexes.stream().filter(index -> ids.contains(index.getId())).collect(Collectors.toList());
    }

    @Transaction(project = 0)
    public void removeIndex(String project, String model, final long id, IndexEntity.Range indexRange) {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(model);
        if (modelDesc.showFusionModel()) {
            FusionModel fusionModel = getFusionModelManager(project).getFusionModel(model);
            String batchId = fusionModel.getBatchModel().getUuid();
            if (IndexEntity.Range.BATCH == indexRange) {
                indexPlanService.removeIndex(project, batchId, id);
                return;
            } else if (IndexEntity.Range.HYBRID == indexRange) {
                indexPlanService.removeIndex(project, batchId, id);
            }
        }
        indexPlanService.removeIndex(project, model, id);
    }
}
