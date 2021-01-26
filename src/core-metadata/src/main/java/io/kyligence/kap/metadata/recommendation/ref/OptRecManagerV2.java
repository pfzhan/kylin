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

package io.kyligence.kap.metadata.recommendation.ref;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.cube.optimization.GarbageLayoutType;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.recommendation.candidate.LayoutMetric;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptRecManagerV2 {

    private static class CCConflictHandlerV2 extends ComputedColumnUtil.BasicCCConflictHandler {
        @Override
        public void handleOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
                ComputedColumnDesc newCC) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_EXPRESSION_CONFLICT(newCC.getExpression(), newCC.getColumnName(),
                            existingCC.getColumnName()));
        }

        @Override
        public void handleOnSameNameDiffExpr(NDataModel existingModel, NDataModel newModel,
                ComputedColumnDesc existingCC, ComputedColumnDesc newCC) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_NAME_CONFLICT(newCC.getColumnName()));
        }
    }

    // for user defined
    public void checkCCName(NDataModel model, ComputedColumnDesc cc) {
        val otherModels = NDataModelManager.getInstance(config, project).listAllModels().stream()
                .filter(m -> !m.getId().equals(model.getId()) && !m.isBroken()).collect(Collectors.toList());

        for (NDataModel otherModel : otherModels) {
            for (ComputedColumnDesc existCC : otherModel.getComputedColumnDescs()) {
                ComputedColumnUtil.singleCCConflictCheck(otherModel, model, existCC, cc, new CCConflictHandlerV2());
            }
        }
        Set<String> measureNameSet = Sets.newHashSet();
        Set<String> columnNameSet = Sets.newHashSet();
        Set<String> dimensionSet = Sets.newHashSet();
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        for (ComputedColumnDesc computedColumn : model.getComputedColumnDescs()) {
            ccMap.putIfAbsent(computedColumn.getColumnName().toUpperCase(Locale.ROOT), computedColumn);
        }
        for (NDataModel.NamedColumn column : model.getAllNamedColumns()) {
            TblColRef tblColRef = model.getEffectiveCols().get(column.getId());
            if (tblColRef == null) {
                continue;
            }
            ColumnDesc columnDesc = tblColRef.getColumnDesc();
            if (!column.isExist() || columnDesc.isComputedColumn()) {
                continue;
            }

            if (column.isDimension()) {
                dimensionSet.add(column.getName().toUpperCase(Locale.ROOT));
            } else {
                columnNameSet.add(column.getName().toUpperCase(Locale.ROOT));
            }
        }
        for (NDataModel.Measure modelAllMeasure : model.getAllMeasures()) {
            if (!modelAllMeasure.isTomb()) {
                measureNameSet.add(modelAllMeasure.getName().toUpperCase(Locale.ROOT));
            }
        }

        if (ccMap.containsKey(cc.getColumnName().toUpperCase(Locale.ROOT))) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_NAME_CONFLICT(cc.getColumnName()));
        } else if (dimensionSet.contains(cc.getColumnName().toUpperCase(Locale.ROOT))) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_DIMENSION_NAME_CONFLICT(cc.getColumnName()));
        } else if (columnNameSet.contains(cc.getColumnName().toUpperCase(Locale.ROOT))) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_COLUMN_NAME_CONFLICT(cc.getColumnName()));
        } else if (measureNameSet.contains(cc.getColumnName().toUpperCase(Locale.ROOT))) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_MEASURE_NAME_CONFLICT(cc.getColumnName()));
        }
    }

    public void checkMeasureName(NDataModel model, NDataModel.Measure measure) {
        Set<String> measureSet = model.getAllMeasures().stream() //
                .filter(measure1 -> !measure1.isTomb()) //
                .map(MeasureDesc::getName).collect(Collectors.toSet());
        if (measureSet.contains(measure.getName())) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getMEASURE_CONFLICT(measure.getName()));
        }
    }

    public void checkDimensionName(Map<Integer, NDataModel.NamedColumn> columns) {
        columns.values().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getName, Function.identity(), (u, v) -> {
                    if (u == v) {
                        return u;
                    }
                    throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                            MsgPicker.getMsg().getDIMENSION_CONFLICT(v.getName()));
                }));
    }

    public static OptRecManagerV2 getInstance(String project) {
        return Singletons.getInstance(project, OptRecManagerV2.class);
    }

    private final KylinConfig config;
    private final String project;

    OptRecManagerV2(String project) {
        this.config = KylinConfig.getInstanceFromEnv();
        this.project = project;
    }

    public OptRecV2 loadOptRecV2(String uuid) {
        Preconditions.checkState(StringUtils.isNotEmpty(uuid));
        OptRecV2 optRecV2 = new OptRecV2(project, uuid);
        List<Integer> brokenLayoutIds = Lists.newArrayList(optRecV2.getBrokenLayoutRefIds());
        if (!brokenLayoutIds.isEmpty()) {
            log.debug("recognized broken index ids: {}", brokenLayoutIds);
            RawRecManager.getInstance(project).removeByIds(brokenLayoutIds);
        }
        return optRecV2;
    }

    public void discardAll(String uuid) {
        OptRecV2 optRecV2 = loadOptRecV2(uuid);
        List<Integer> rawIds = optRecV2.getRawIds();
        Map<Integer, RawRecItem> rawRecItemMap = optRecV2.getRawRecItemMap();
        List<Integer> layoutRawIds = Lists.newArrayList();
        rawIds.forEach(recId -> {
            if (rawRecItemMap.get(recId).isLayoutRec()) {
                layoutRawIds.add(recId);
            }
        });
        RawRecManager rawManager = RawRecManager.getInstance(project);
        rawManager.discardByIds(layoutRawIds);
    }

    public void genRecItemsFromIndexOptimizer(String project, String modelId,
            Map<Long, GarbageLayoutType> garbageLayouts) {
        if (garbageLayouts.isEmpty()) {
            return;
        }
        log.info("Generating raw recommendations from index optimizer for model({}/{})", project, modelId);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        NDataModel model = modelManager.getDataModelDesc(modelId);

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        Map<Long, LayoutEntity> allLayoutsMap = indexPlan.getAllLayoutsMap();

        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        Map<Long, FrequencyMap> hitFrequencyMap = dataflow.getLayoutHitCount();

        RawRecManager recManager = RawRecManager.getInstance(project);
        Map<String, RawRecItem> layoutRecommendations = recManager.queryNonAppliedLayoutRawRecItems(modelId, false);
        Map<String, String> uniqueFlagToUuid = Maps.newHashMap();
        layoutRecommendations.forEach((k, v) -> {
            LayoutRecItemV2 recEntity = (LayoutRecItemV2) v.getRecEntity();
            uniqueFlagToUuid.put(recEntity.getLayout().genUniqueContent(), k);
        });
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        garbageLayouts.forEach((layoutId, type) -> {
            LayoutEntity layout = allLayoutsMap.get(layoutId);
            String uniqueString = layout.genUniqueContent();
            String uuid = uniqueFlagToUuid.get(uniqueString);
            FrequencyMap frequencyMap = hitFrequencyMap.getOrDefault(layoutId, new FrequencyMap());
            RawRecItem recItem;
            if (uniqueFlagToUuid.containsKey(uniqueString)) {
                recItem = layoutRecommendations.get(uuid);
                recItem.setUpdateTime(System.currentTimeMillis());
                recItem.setRecSource(type.name());
                if (recItem.getState() == RawRecItem.RawRecState.DISCARD) {
                    recItem.setState(RawRecItem.RawRecState.INITIAL);
                    LayoutMetric layoutMetric = recItem.getLayoutMetric();
                    if (layoutMetric == null) {
                        recItem.setLayoutMetric(new LayoutMetric(frequencyMap, new LayoutMetric.LatencyMap()));
                    } else {
                        layoutMetric.setFrequencyMap(frequencyMap);
                    }
                }
            } else {
                LayoutRecItemV2 item = new LayoutRecItemV2();
                item.setLayout(layout);
                item.setCreateTime(System.currentTimeMillis());
                item.setAgg(layout.getId() < IndexEntity.TABLE_INDEX_START_ID);
                item.setUuid(UUID.randomUUID().toString());

                recItem = new RawRecItem(project, modelId, model.getSemanticVersion(),
                        RawRecItem.RawRecType.REMOVAL_LAYOUT);
                recItem.setRecEntity(item);
                recItem.setCreateTime(item.getCreateTime());
                recItem.setUpdateTime(item.getCreateTime());
                recItem.setState(RawRecItem.RawRecState.INITIAL);
                recItem.setUniqueFlag(item.getUuid());
                recItem.setDependIDs(item.genDependIds());
                recItem.setLayoutMetric(new LayoutMetric(frequencyMap, new LayoutMetric.LatencyMap()));
                recItem.setRecSource(type.name());
            }

            if (recItem.getLayoutMetric() != null) {
                rawRecItems.add(recItem);
            }
        });
        RawRecManager.getInstance(project).saveOrUpdate(rawRecItems);
        log.info("Raw recommendations from index optimizer for model({}/{}) successfully generated.", project, modelId);
    }
}
