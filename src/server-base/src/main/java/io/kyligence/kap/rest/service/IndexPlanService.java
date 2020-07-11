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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_AGG_INDEX;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_TABLE_INDEX;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kyligence.kap.metadata.recommendation.v2.OptimizeRecommendationManagerV2;
import lombok.var;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.OutOfMaxCombinationException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.response.AggIndexCombResult;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.rest.request.AggShardByColumnsRequest;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.AggShardByColumnsResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.IndexGraphResponse;
import io.kyligence.kap.rest.response.IndexResponse;
import io.kyligence.kap.rest.response.TableIndexResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("indexPlanService")
public class IndexPlanService extends BasicService {

    public static final String DATA_SIZE = "data_size";
    public static final String LAST_MODIFIED_TIME = "last_modified_time";

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpater;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Transaction(project = 0)
    public Pair<IndexPlan, BuildIndexResponse> updateRuleBasedCuboid(String project,
            final UpdateRuleBasedCuboidRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        try {
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val indexPlanManager = getIndexPlanManager(project);
            val modelManager = NDataModelManager.getInstance(kylinConfig, request.getProject());
            IndexPlan originIndexPlan = getIndexPlan(request.getProject(), request.getModelId());
            val model = modelManager.getDataModelDesc(request.getModelId());

            Preconditions.checkNotNull(model);

            val indexPlan = indexPlanManager.updateIndexPlan(originIndexPlan.getUuid(), copyForWrite -> {
                val newRuleBasedCuboid = request.convertToRuleBasedIndex();
                newRuleBasedCuboid.setLastModifiedTime(System.currentTimeMillis());
                copyForWrite.setRuleBasedIndex(newRuleBasedCuboid, false, true);
            });
            BuildIndexResponse response = new BuildIndexResponse();
            if (request.isLoadData()) {
                response = semanticUpater.handleIndexPlanUpdateRule(request.getProject(), model.getUuid(),
                        originIndexPlan.getRuleBasedIndex(), indexPlan.getRuleBasedIndex(), false);
            }
            cleanInEffectiveRecommendation(project, request.getModelId());
            return new Pair<>(indexPlanManager.getIndexPlan(originIndexPlan.getUuid()), response);
        } catch (Exception e) {
            throw new KylinException(FAILED_UPDATE_AGG_INDEX, e);
        }
    }

    private void cleanInEffectiveRecommendation(String project, String modelId) {
        val prjManager = getProjectManager();
        val prjInstance = prjManager.getProject(project);
        if (prjInstance.isSemiAutoMode()) {
            val recommendationManager = getOptimizeRecommendationManager(project);
            recommendationManager.cleanInEffective(modelId);
            val recommendationManagerV2 = getOptimizeRecommendationManagerV2(project);
            recommendationManagerV2.cleanInEffective(modelId);
        }
    }

    @Transaction(project = 0)
    public BuildIndexResponse updateTableIndex(String project, CreateTableIndexRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        try {
            val indexPlan = getIndexPlan(request.getProject(), request.getModelId());
            val layout = parseToLayout(project, request);
            for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
                if (cuboidLayout.equals(layout) && cuboidLayout.isManual()) {
                    return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_LAYOUT);
                }
            }

            NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataflow(request.getModelId());

            val readySegs = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
            if (readySegs.isEmpty()) {
                removeIndex(project, request.getModelId(), request.getId());
            } else {
                NDataSegment segment = readySegs.getLatestReadySegment();
                NDataLayout dataLayout = segment.getLayout(request.getId());
                // may no data before the last ready segments but have a add cuboid job.
                if (null == dataLayout) {
                    removeIndex(project, request.getModelId(), request.getId());
                } else {
                    addTableIndexToBeDeleted(project, request.getModelId(), Lists.newArrayList(request.getId()));
                }
            }

            return createTableIndex(project, request);
        } catch (Exception e) {
            throw new KylinException(FAILED_UPDATE_TABLE_INDEX, e);
        }
    }

    private LayoutEntity parseToLayout(String project, CreateTableIndexRequest request) {
        val indexPlan = getIndexPlan(request.getProject(), request.getModelId());
        NDataModel model = indexPlan.getModel();

        val newLayout = new LayoutEntity();
        newLayout.setId(indexPlan.getNextTableIndexId() + 1);

        // handle remove the latest table index
        if (Objects.equals(newLayout.getId(), request.getId())) {
            newLayout.setId(newLayout.getId() + IndexEntity.INDEX_ID_STEP);
        }
        newLayout.setColOrder(convertColumn(request.getColOrder(), model));
        newLayout.setStorageType(request.getStorageType());
        newLayout.setShardByColumns(convertColumn(request.getShardByColumns(), model));
        newLayout.setSortByColumns(convertColumn(request.getSortByColumns(), model));
        newLayout.setUpdateTime(System.currentTimeMillis());
        newLayout.setOwner(getUsername());
        newLayout.setManual(true);

        Map<Integer, String> layoutOverride = Maps.newHashMap();
        if (request.getLayoutOverrideIndexes() != null) {
            for (Map.Entry<String, String> entry : request.getLayoutOverrideIndexes().entrySet()) {
                layoutOverride.put(model.getColumnIdByColumnName(entry.getKey()), entry.getValue());
            }
        }
        newLayout.setLayoutOverrideIndexes(layoutOverride);
        return newLayout;
    }

    @Transaction(project = 0)
    public BuildIndexResponse createTableIndex(String project, CreateTableIndexRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        val indexPlanManager = getIndexPlanManager(project);
        val jobManager = getJobManager(project);
        val indexPlan = getIndexPlan(request.getProject(), request.getModelId());
        val newLayout = parseToLayout(project, request);
        for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
            if (cuboidLayout.equals(newLayout) && cuboidLayout.isManual()) {
                throw new IllegalStateException(MsgPicker.getMsg().getDUPLICATEP_LAYOUT());

            }
        }
        int layoutIndex = indexPlan.getWhitelistLayouts().indexOf(newLayout);
        if (layoutIndex != -1) {
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                val oldLayout = copyForWrite.getWhitelistLayouts().get(layoutIndex);
                oldLayout.setManual(true);
                oldLayout.setOwner(getUsername());
                oldLayout.setUpdateTime(System.currentTimeMillis());
            });
            cleanInEffectiveRecommendation(project, request.getModelId());
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_LAYOUT);
        } else {
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                val newCuboid = new IndexEntity();
                newCuboid.setId(newLayout.getId() - 1);
                newCuboid.setDimensions(Lists.newArrayList(newLayout.getColOrder()));
                newCuboid.setLayouts(Arrays.asList(newLayout));
                newCuboid.setIndexPlan(copyForWrite);
                IndexEntity lookForIndex = copyForWrite.getWhiteListIndexesMap().get(newCuboid.createIndexIdentifier());
                if (lookForIndex == null) {
                    copyForWrite.getIndexes().add(newCuboid);
                } else {
                    IndexEntity realIndex = copyForWrite.getIndexes()
                            .get(copyForWrite.getIndexes().indexOf(lookForIndex));
                    newLayout.setId(realIndex.getId() + realIndex.getNextLayoutOffset());
                    realIndex.setNextLayoutOffset((realIndex.getNextLayoutOffset() + 1) % IndexEntity.INDEX_ID_STEP);
                    realIndex.getLayouts().add(newLayout);
                }
            });
            cleanInEffectiveRecommendation(project, request.getModelId());
            if (request.isLoadData()) {
                val df = getDataflowManager(project).getDataflow(request.getModelId());
                val readySegs = df.getSegments();
                if (readySegs.isEmpty()) {
                    return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_SEGMENT);
                }
                getSourceUsageManager().licenseCheckWrap(project, () -> jobManager.addFullIndexJob(indexPlan.getUuid(), getUsername()));
                return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NORM_BUILD);
            }
        }

        return new BuildIndexResponse();
    }

    @Deprecated
    @Transaction(project = 0)
    public void removeTableIndex(String project, String model, final long id) {
        aclEvaluate.checkProjectWritePermission(project);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        if (id < IndexEntity.TABLE_INDEX_START_ID) {
            throw new IllegalStateException("Table Index Id should large than " + IndexEntity.TABLE_INDEX_START_ID);
        }
        val layout = indexPlan.getCuboidLayout(id);
        Preconditions.checkNotNull(layout);
        Preconditions.checkState(layout.isManual());

        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(id), false, true);
        });
    }

    @Transaction(project = 0)
    public void removeIndexes(String project, String model, final Set<Long> ids) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(ids)) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getLAYOUT_LIST_IS_EMPTY());
        }

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkNotNull(indexPlan);

        String notExistsLayoutIds = ids.stream().filter(id -> Objects.isNull(indexPlan.getCuboidLayout(id))).sorted()
                .map(String::valueOf).collect(Collectors.joining(","));

        if (StringUtils.isNotEmpty(notExistsLayoutIds)) {
            throw new KylinException(INVALID_PARAMETER,
                    String.format(MsgPicker.getMsg().getLAYOUT_NOT_EXISTS(), notExistsLayoutIds));
        }

        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            for (Long id : ids) {
                val layout = indexPlan.getCuboidLayout(id);
                if (id < IndexEntity.TABLE_INDEX_START_ID && layout.isManual()) {
                    copyForWrite.addRuleBasedBlackList(Lists.newArrayList(layout.getId()));
                }
            }

            copyForWrite.removeLayouts(ids, true, true);
        });
        val recommendationManager = OptimizeRecommendationManager.getInstance(kylinConfig, project);
        recommendationManager.cleanInEffective(model);
        val recommendationManagerV2 = OptimizeRecommendationManagerV2.getInstance(kylinConfig, project);
        recommendationManagerV2.cleanInEffective(model);

    }

    @Transaction(project = 0)
    public void removeIndex(String project, String model, final long id) {
        removeIndexes(project, model, Collections.singleton(id));
    }

    private boolean addTableIndexToBeDeleted(String project, String modelId, Collection<Long> layoutIds) {
        aclEvaluate.checkProjectWritePermission(project);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

        val indexPlan = getIndexPlan(project, modelId);
        Preconditions.checkNotNull(indexPlan);
        for (Long id : layoutIds) {
            val layout = indexPlan.getCuboidLayout(id);
            Preconditions.checkNotNull(layout);
        }

        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.markTableIndexesToBeDeleted(indexPlan.getUuid(), Sets.newHashSet(layoutIds));
        });

        return true;
    }

    public DiffRuleBasedIndexResponse calculateDiffRuleBasedIndex(UpdateRuleBasedCuboidRequest request) {
        aclEvaluate.checkProjectWritePermission(request.getProject());
        Pair<Set<LayoutEntity>, Set<LayoutEntity>> diff = getIndexPlan(request.getProject(), request.getModelId())
                .diffRuleBasedIndex(request.convertToRuleBasedIndex());

        return new DiffRuleBasedIndexResponse(request.getModelId(), diff.getFirst().size(), diff.getSecond().size());
    }

    public AggIndexResponse calculateAggIndexCount(UpdateRuleBasedCuboidRequest request) {
        aclEvaluate.checkProjectWritePermission(request.getProject());
        val maxCount = getConfig().getCubeAggrGroupMaxCombination();
        List<NAggregationGroup> aggregationGroups = request.getAggregationGroups();
        val indexPlan = getIndexPlan(request.getProject(), request.getModelId()).copy();
        AggIndexCombResult totalResult;
        AggIndexCombResult aggIndexResult;

        val aggregationGroupsCopy = aggregationGroups.stream()
                .filter(aggGroup -> aggGroup.getIncludes() != null && aggGroup.getIncludes().length != 0)
                .collect(Collectors.toList());
        request.setAggregationGroups(aggregationGroupsCopy);

        boolean invalid = false;
        try {
            NRuleBasedIndex ruleBasedIndex = request.convertToRuleBasedIndex();
            NDataModel model = NDataModelManager.getInstance(getConfig(), request.getProject())
                    .getDataModelDesc(indexPlan.getUuid());

            if (CollectionUtils.isNotEmpty(ruleBasedIndex.getDimensions())) {
                List<Integer> notExistCols = ruleBasedIndex.getDimensions().stream()
                        .filter(col -> null == model.getEffectiveDimensions()
                                || null == model.getEffectiveDimensions().get(col))
                        .collect(Collectors.toList());

                if (CollectionUtils.isNotEmpty(notExistCols)) {
                    throw new KylinException(ServerErrorCode.EFFECTIVE_DIMENSION_NOT_FIND,
                            String.format(MsgPicker.getMsg().getEFFECTIVE_DIMENSION_NOT_FIND(),
                                    StringUtils.join(notExistCols.iterator(), ",")));
                }
            }

            indexPlan.setRuleBasedIndex(ruleBasedIndex);
        } catch (OutOfMaxCombinationException oe) {
            invalid = true;
            log.error("The number of cuboid for the cube exceeds the limit, ", oe);
        } catch (IllegalStateException e) {
            log.error(e.getMessage());
        }

        List<AggIndexCombResult> aggIndexCounts = Lists.newArrayList();
        for (NAggregationGroup group : aggregationGroups) {
            if (group.getIncludes() != null && group.getIncludes().length == 0) {
                aggIndexResult = AggIndexCombResult.successResult(0L);
                aggIndexCounts.add(aggIndexResult);
                continue;
            }
            long count = group.calculateCuboidCombination();
            if (count > maxCount) {
                aggIndexResult = AggIndexCombResult.errorResult();
                invalid = true;
            } else {
                aggIndexResult = AggIndexCombResult.successResult(count);
            }
            aggIndexCounts.add(aggIndexResult);
        }

        if (invalid) {
            totalResult = AggIndexCombResult.errorResult();
        } else {
            try {
                long cuboidCount = indexPlan.getRuleBasedIndex().getInitialCuboidScheduler().getCuboidCount();
                totalResult = AggIndexCombResult.successResult(cuboidCount);
            } catch (OutOfMaxCombinationException outOfMaxCombinationException) {
                totalResult = AggIndexCombResult.errorResult();
            }
        }
        return new AggIndexResponse(aggIndexCounts, totalResult, getConfig().getCubeAggrGroupMaxCombination());
    }

    public void checkIndexCountWithinLimit(UpdateRuleBasedCuboidRequest request) {
        val maxCount = getConfig().getCubeAggrGroupMaxCombination();
        List<NAggregationGroup> aggGroups = request.getAggregationGroups();

        val indexPlan = getIndexPlan(request.getProject(), request.getModelId()).copy();

        val aggregationGroupsCopy = aggGroups.stream()
                .filter(aggGroup -> aggGroup.getIncludes() != null && aggGroup.getIncludes().length != 0)
                .collect(Collectors.toList());
        request.setAggregationGroups(aggregationGroupsCopy);

        try {
            indexPlan.setRuleBasedIndex(request.convertToRuleBasedIndex());
        } catch (OutOfMaxCombinationException oe) {
            log.error("The number of cuboid for the cube exceeds the limit, ", oe);
        } catch (IllegalStateException e) {
            log.error(e.getMessage());
        }

        for (NAggregationGroup aggGroup : aggGroups) {
            long count = aggGroup.calculateCuboidCombination();
            if (count > maxCount) {
                throw new IllegalArgumentException(
                        "The aggregate amount exceeds its limit per aggregate group, please optimize the group setting or reduce dimension amount.");
            }
        }
    }

    @Transaction(project = 0)
    public void updateShardByColumns(String project, AggShardByColumnsRequest request) {
        aclEvaluate.checkProjectWritePermission(project);

        val modelId = request.getModelId();
        val indexPlanManager = getIndexPlanManager(project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        val model = indexPlan.getModel();

        val dimensions = model.getDimensionNameIdMap();
        for (String shardByColumn : request.getShardByColumns()) {
            if (!dimensions.containsKey(shardByColumn)) {
                throw new KylinException(PERMISSION_DENIED,
                        String.format(MsgPicker.getMsg().getCOLUMU_IS_NOT_DIMENSION(), shardByColumn));
            }
        }

        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.setAggShardByColumns(request.getShardByColumns().stream().map(model::getColumnIdByColumnName)
                    .collect(Collectors.toList()));
        });
        if (request.isLoadData()) {
            val jobManager = getJobManager(project);
            getSourceUsageManager().licenseCheckWrap(project, () -> jobManager.addFullIndexJob(modelId, getUsername()));
        }
    }

    public AggShardByColumnsResponse getShardByColumns(String project, String modelId) {
        aclEvaluate.checkProjectWritePermission(project);
        val indexPlanManager = getIndexPlanManager(project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        val model = indexPlan.getModel();
        val result = new AggShardByColumnsResponse();
        result.setModelId(modelId);
        result.setProject(project);
        result.setShardByColumns(indexPlan.getAggShardByColumns().stream().map(model::getColumnNameByColumnId)
                .collect(Collectors.toList()));
        return result;
    }

    public List<TableIndexResponse> getTableIndexs(String project, String model) {
        aclEvaluate.checkProjectReadPermission(project);
        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        List<TableIndexResponse> result = Lists.newArrayList();
        for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
            if (IndexEntity.isTableIndex(cuboidLayout.getId())) {
                result.add(convertToTableIndexResponse(cuboidLayout, indexPlan.getModel()));
            }
        }
        return result;
    }

    public List<IndexResponse> getIndexes(String project, String modelId, String key, List<String> status,
            String orderBy, Boolean desc, List<IndexResponse.Source> sources) {
        aclEvaluate.checkProjectReadPermission(project);
        Set<IndexResponse.Status> statusSet = Sets.newHashSet();
        Optional.ofNullable(status).ifPresent(stringStatus -> statusSet
                .addAll(stringStatus.stream().map(IndexResponse.Status::valueOf).collect(Collectors.toSet())));

        val indexPlan = getIndexPlan(project, modelId);
        Preconditions.checkState(indexPlan != null);
        val model = indexPlan.getModel();
        val layouts = indexPlan.getAllLayouts();
        Set<Long> layoutsByRunningJobs = getLayoutsByRunningJobs(project, modelId);
        if (StringUtils.isBlank(key)) {
            return sortAndFilterLayouts(layouts.stream()
                    .map(layoutEntity -> convertToResponse(layoutEntity, indexPlan.getModel(), layoutsByRunningJobs))
                    .filter(indexResponse -> statusSet.isEmpty() || statusSet.contains(indexResponse.getStatus())),
                    orderBy, desc, sources);
        }

        String trimmedKey = key.trim();
        val matchCCs = model.getComputedColumnDescs().stream()
                .filter(cc -> containsIgnoreCase(cc.getFullName(), trimmedKey)
                        || containsIgnoreCase(cc.getInnerExpression(), trimmedKey))
                .map(ComputedColumnDesc::getFullName).collect(Collectors.toSet());
        val matchDimensions = model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .filter(c -> containsIgnoreCase(c.getAliasDotColumn(), trimmedKey)
                        || containsIgnoreCase(c.getName(), trimmedKey) || matchCCs.contains(c.getAliasDotColumn()))
                .map(NDataModel.NamedColumn::getId).collect(Collectors.toSet());
        val matchMeasures = model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .filter(m -> containsIgnoreCase(m.getName(), trimmedKey) || m.getFunction().getParameters().stream()
                        .anyMatch(p -> p.getType().equals(FunctionDesc.PARAMETER_TYPE_COLUMN)
                                && (containsIgnoreCase(p.getValue(), trimmedKey) || matchCCs.contains(p.getValue()))))
                .map(NDataModel.Measure::getId).collect(Collectors.toSet());

        return sortAndFilterLayouts(layouts.stream().filter(index -> {
            val cols = Sets.newHashSet(index.getColOrder());
            return String.valueOf(index.getId()).equals(trimmedKey)
                    || !Sets.intersection(matchDimensions, cols).isEmpty()
                    || !Sets.intersection(matchMeasures, cols).isEmpty();
        }).map(layoutEntity -> convertToResponse(layoutEntity, indexPlan.getModel(), layoutsByRunningJobs))
                .filter(indexResponse -> statusSet.isEmpty() || statusSet.contains(indexResponse.getStatus())), orderBy,
                desc, sources);
    }

    private boolean containsIgnoreCase(String s1, String s2) {
        return s1.toUpperCase().contains(s2.toUpperCase());
    }

    public IndexGraphResponse getIndexGraph(String project, String modelId, int maxSize) {
        aclEvaluate.checkProjectReadPermission(project);
        val indexPlan = getIndexPlan(project, modelId);
        Preconditions.checkNotNull(indexPlan);
        val indexes = indexPlan.getAllLayouts();
        val indexResponses = sortAndFilterLayouts(
                indexes.stream().map(layoutEntity -> convertToResponse(layoutEntity, indexPlan.getModel())), DATA_SIZE,
                true, Lists.newArrayList());
        val indexGraphResponse = new IndexGraphResponse();

        indexGraphResponse.setProject(project);
        indexGraphResponse.setModel(modelId);
        indexGraphResponse.setTotalIndexes(indexResponses.size());
        indexGraphResponse.setEmptyIndexes(
                indexResponses.stream().filter(r -> r.getStatus().equals(IndexResponse.Status.EMPTY)).count());

        Function<IndexResponse, IndexGraphResponse.Index> responseConverter = res -> {
            val index = new IndexGraphResponse.Index();
            BeanUtils.copyProperties(res, index);
            return index;
        };
        indexGraphResponse.setAutoAggIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> r.getSource().equals(IndexResponse.Source.AUTO_AGG)).map(responseConverter)));
        indexGraphResponse.setManualAggIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> r.getSource().equals(IndexResponse.Source.MANUAL_AGG)).map(responseConverter)));
        indexGraphResponse.setAutoTableIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> r.getSource().equals(IndexResponse.Source.AUTO_TABLE)).map(responseConverter)));
        indexGraphResponse.setManualTableIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> r.getSource().equals(IndexResponse.Source.MANUAL_TABLE)).map(responseConverter)));

        val dataflow = NDataflowManager.getInstance(indexPlan.getConfig(), indexPlan.getProject())
                .getDataflow(indexPlan.getId());
        // include all segs (except refreshing, merging) for data range count
        val readySegments = dataflow.getSegments().getSegmentsExcludeRefreshingAndMerging().stream()
                .collect(Collectors.toCollection(Segments::new));
        long startTime = 0;
        long endTime = 0;
        if (!readySegments.isEmpty()) {
            startTime = Long.MAX_VALUE;
            for (NDataSegment seg : readySegments) {
                long start = Long.parseLong(seg.getSegRange().getStart().toString());
                long end = Long.parseLong(seg.getSegRange().getEnd().toString());
                startTime = Math.min(startTime, start);
                endTime = Math.max(endTime, end);
            }
        }
        indexGraphResponse.setStartTime(startTime);
        indexGraphResponse.setEndTime(endTime);

        long segmentToComplementCount = 0;
        for (NDataSegment seg : readySegments) {
            if (seg.getSegDetails().getLayouts().size() != indexPlan.getAllLayouts().size()) {
                segmentToComplementCount += 1;
            }
        }
        indexGraphResponse.setSegmentToComplementCount(segmentToComplementCount);

        return indexGraphResponse;
    }

    private List<IndexResponse> sortAndFilterLayouts(Stream<IndexResponse> layouts, String orderBy, boolean reverse,
            List<IndexResponse.Source> sources) {
        if (CollectionUtils.isNotEmpty(sources)) {
            layouts = layouts.filter(r -> sources.contains(r.getSource()));
        }
        Comparator<IndexResponse> comparator;
        // default EMPTY layout at first then order by data_size ascending;
        if (StringUtils.isEmpty(orderBy)) {
            comparator = Comparator.<IndexResponse> comparingInt(res -> res.getStatus().ordinal())
                    .thenComparing(propertyComparator(DATA_SIZE, true));
        } else {
            comparator = propertyComparator(orderBy, !reverse);
        }
        return layouts.sorted(comparator).collect(Collectors.toList());
    }

    private IndexGraphResponse.IndexList convertToIndexList(Stream<IndexGraphResponse.Index> stream) {
        val indexList = new IndexGraphResponse.IndexList();
        indexList.setIndexes(stream.collect(Collectors.toList()));
        indexList.setTotalSize(indexList.getIndexes().stream().mapToLong(IndexGraphResponse.Index::getDataSize).sum());
        return indexList;

    }

    public NRuleBasedIndex getRule(String project, String model) {
        aclEvaluate.checkProjectWritePermission(project);
        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        return indexPlan.getRuleBasedIndex();
    }

    private TableIndexResponse convertToTableIndexResponse(LayoutEntity cuboidLayout, NDataModel model) {
        val response = new TableIndexResponse();
        BeanUtils.copyProperties(cuboidLayout, response);
        response.setColOrder(convertColumnIdName(cuboidLayout.getColOrder(), model));
        response.setShardByColumns(convertColumnIdName(cuboidLayout.getShardByColumns(), model));
        response.setSortByColumns(convertColumnIdName(cuboidLayout.getSortByColumns(), model));
        response.setProject(model.getProject());
        response.setModel(model.getUuid());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dfMgr.getDataflow(cuboidLayout.getIndex().getIndexPlan().getUuid());
        TableIndexResponse.Status status = TableIndexResponse.Status.AVAILABLE;
        int readyCount = 0;
        for (NDataSegment segment : dataflow.getSegments()) {
            val dataCuboid = segment.getLayout(cuboidLayout.getId());
            if (dataCuboid == null) {
                continue;
            }
            readyCount++;
        }
        if (readyCount != dataflow.getSegments().size() || CollectionUtils.isEmpty(dataflow.getSegments())) {
            status = TableIndexResponse.Status.EMPTY;
        }
        response.setStatus(status);
        response.setUpdateTime(cuboidLayout.getUpdateTime());
        return response;
    }

    @VisibleForTesting
    public Set<Long> getLayoutsByRunningJobs(String project, String modelId) {
        List<AbstractExecutable> runningJobList = NExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).getExecutablesByStatusList(Sets.newHashSet(
                        ExecutableState.READY, ExecutableState.RUNNING, ExecutableState.PAUSED, ExecutableState.ERROR));

        return runningJobList.stream()
                .filter(abstractExecutable -> Objects.equals(modelId, abstractExecutable.getTargetSubject()))
                .map(AbstractExecutable::getToBeDeletedLayoutIds).flatMap(Set::stream).collect(Collectors.toSet());
    }

    private IndexResponse convertToResponse(LayoutEntity layoutEntity, NDataModel model) {
        return convertToResponse(layoutEntity, model, Sets.newHashSet());
    }

    private IndexResponse convertToResponse(LayoutEntity layoutEntity, NDataModel model,
            Set<Long> layoutIdsOfRunningJobs) {
        val response = new IndexResponse();
        BeanUtils.copyProperties(layoutEntity, response);
        response.setColOrder(convertColumnOrMeasureIdName(layoutEntity.getColOrder(), model));
        response.setShardByColumns(convertColumnIdName(layoutEntity.getShardByColumns(), model));
        response.setSortByColumns(convertColumnIdName(layoutEntity.getSortByColumns(), model));
        response.setProject(model.getProject());
        response.setModel(model.getUuid());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dfMgr.getDataflow(layoutEntity.getIndex().getIndexPlan().getUuid());
        long dataSize = 0L;
        int readyCount = 0;
        for (NDataSegment segment : dataflow.getSegments()) {
            val dataCuboid = segment.getLayout(layoutEntity.getId());
            if (dataCuboid == null) {
                continue;
            }
            readyCount++;
            dataSize += dataCuboid.getByteSize();
        }

        IndexResponse.Status status;
        if (readyCount <= 0) {
            status = IndexResponse.Status.EMPTY;
            if (layoutIdsOfRunningJobs.contains(layoutEntity.getId())) {
                status = IndexResponse.Status.BUILDING;
            }
        } else {
            status = IndexResponse.Status.AVAILABLE;
        }

        if (layoutEntity.isToBeDeleted()) {
            status = IndexResponse.Status.TO_BE_DELETED;
        }
        response.setStatus(status);
        response.setDataSize(dataSize);
        response.setLastModifiedTime(layoutEntity.getUpdateTime());
        if (dataflow.getLayoutHitCount().get(layoutEntity.getId()) != null) {
            response.setUsage(dataflow.getLayoutHitCount().get(layoutEntity.getId()).getDateFrequency().values()
                    .stream().mapToInt(Integer::intValue).sum());
        }
        return response;
    }

    private IndexPlan getIndexPlan(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);
        return indexPlanManager.getIndexPlan(model);
    }

    private List<String> convertColumnIdName(List<Integer> ids, NDataModel model) {
        if (CollectionUtils.isEmpty(ids)) {
            return Lists.newArrayList();
        }
        val result = Lists.<String> newArrayList();
        for (Integer columnId : ids) {
            val name = (columnId < NDataModel.MEASURE_ID_BASE) ? model.getColumnNameByColumnId(columnId)
                    : model.getMeasureNameByMeasureId(columnId);
            result.add(name);
        }
        return result;

    }

    private List<IndexResponse.ColOrderPair> convertColumnOrMeasureIdName(List<Integer> ids, NDataModel model) {
        if (CollectionUtils.isEmpty(ids)) {
            return Lists.newArrayList();
        }
        val result = Lists.<IndexResponse.ColOrderPair> newArrayList();
        for (Integer id : ids) {
            if (id < NDataModel.MEASURE_ID_BASE) {
                result.add(new IndexResponse.ColOrderPair(model.getColumnNameByColumnId(id), "column"));
            } else {
                result.add(new IndexResponse.ColOrderPair(model.getMeasureNameByMeasureId(id), "measure"));
            }
        }
        return result;

    }

    private List<Integer> convertColumn(List<String> columns, NDataModel model) {
        if (CollectionUtils.isEmpty(columns)) {
            return Lists.newArrayList();
        }
        val result = Lists.<Integer> newArrayList();
        for (String column : columns) {
            val id = model.getColumnIdByColumnName(column);
            result.add(id);
        }
        return result;
    }

    public void reloadLayouts(String project, String modelId, Set<Long> changedLayouts) {
        getIndexPlanManager(project).updateIndexPlan(modelId, copy -> {
            val indexes = changedLayouts.stream()
                    .map(layout -> (layout / IndexEntity.INDEX_ID_STEP) * IndexEntity.INDEX_ID_STEP)
                    .collect(Collectors.toSet());
            val originAgg = JsonUtil.deepCopyQuietly(copy.getRuleBasedIndex(), NRuleBasedIndex.class);
            copy.addRuleBasedBlackList(
                    copy.getRuleBaseLayouts().stream().filter(l -> changedLayouts.contains(l.getId()))
                            .map(LayoutEntity::getId).collect(Collectors.toList()));
            val changedIndexes = copy.getIndexes().stream().filter(i -> indexes.contains(i.getId()))
                    .collect(Collectors.toList());
            copy.setIndexes(
                    copy.getIndexes().stream().filter(i -> !indexes.contains(i.getId())).collect(Collectors.toList()));
            var nextAggIndexId = copy.getNextAggregationIndexId();
            var nextTableIndexId = copy.getNextTableIndexId();
            for (IndexEntity indexEntity : changedIndexes) {
                var nextLayoutOffset = 1L;
                var nextIndexId = indexEntity.isTableIndex() ? nextTableIndexId : nextAggIndexId;
                indexEntity.setId(nextIndexId);
                if (indexEntity.isTableIndex()) {
                    nextTableIndexId = nextIndexId + IndexEntity.INDEX_ID_STEP;
                } else {
                    nextAggIndexId = nextIndexId + IndexEntity.INDEX_ID_STEP;
                }
                for (LayoutEntity layoutEntity : indexEntity.getLayouts()) {
                    layoutEntity.setId(indexEntity.getId() + (nextLayoutOffset++));
                }
                indexEntity.setNextLayoutOffset(nextLayoutOffset);
            }
            val indexList = copy.getIndexes();
            indexList.addAll(changedIndexes);
            copy.setIndexes(indexList);
            if (originAgg != null) {
                val updatedAgg = new NRuleBasedIndex();
                updatedAgg.setAggregationGroups(originAgg.getAggregationGroups());
                updatedAgg.setGlobalDimCap(originAgg.getGlobalDimCap());
                updatedAgg.setDimensions(originAgg.getDimensions());
                updatedAgg.setLastModifiedTime(System.currentTimeMillis());
                copy.setRuleBasedIndex(updatedAgg, false, false);
            }
        });
    }

}
