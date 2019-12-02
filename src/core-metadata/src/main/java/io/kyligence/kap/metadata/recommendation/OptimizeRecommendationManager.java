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
package io.kyligence.kap.metadata.recommendation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.garbage.GarbageLayoutType;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.alias.AliasMapping;
import io.kyligence.kap.metadata.model.exception.IllegalCCExpressionException;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import lombok.AllArgsConstructor;
import lombok.val;
import lombok.var;

public class OptimizeRecommendationManager {
    private static final Logger logger = LoggerFactory.getLogger(OptimizeRecommendationManager.class);

    public static final String REMOVE_REASON = "remove_reason";

    public static OptimizeRecommendationManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, OptimizeRecommendationManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static OptimizeRecommendationManager newInstance(KylinConfig conf, String project) {

        return new OptimizeRecommendationManager(conf, project);
    }

    public static final int ID_OFFSET = 10000000;
    public static final int MEASURE_OFFSET = ID_OFFSET + NDataModel.MEASURE_ID_BASE;

    public static boolean isVirtualColumnId(int id) {
        return id >= ID_OFFSET;
    }

    public static boolean isVirtualMeasureId(int id) {
        return id >= MEASURE_OFFSET;
    }

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<OptimizeRecommendation> crud;

    public OptimizeRecommendationManager(KylinConfig config, String project) {
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) {
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + ResourceStore.MODEL_OPTIMIZE_RECOMMENDATION;
        this.crud = new CachedCrudAssist<OptimizeRecommendation>(getStore(), resourceRootPath,
                OptimizeRecommendation.class) {
            @Override
            protected OptimizeRecommendation initEntityAfterReload(OptimizeRecommendation entity, String resourceName) {
                entity.init();
                return entity;
            }
        };
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public OptimizeRecommendation getOptimizeRecommendation(String id) {
        if (StringUtils.isEmpty(id)) {
            return null;
        }
        return crud.get(id);
    }

    public int getRecommendationCount(String id) {
        val optRecommendation = getOptimizeRecommendation(id);
        return optRecommendation == null ? 0 : optRecommendation.getRecommendationsCount();
    }

    public OptimizeRecommendation save(OptimizeRecommendation recommendation) {
        return crud.save(recommendation);
    }

    static CCRecommendationItem createRecommendation(ComputedColumnDesc computedColumnDesc) {
        val recommendation = new CCRecommendationItem();
        recommendation.setCc(computedColumnDesc);
        recommendation.setCreateTime(System.currentTimeMillis());
        return recommendation;
    }

    static DimensionRecommendationItem createRecommendation(NDataModel.NamedColumn column) {
        val recommendation = new DimensionRecommendationItem();
        recommendation.setColumn(column);
        recommendation.setCreateTime(System.currentTimeMillis());
        return recommendation;
    }

    static MeasureRecommendationItem createRecommendation(NDataModel.Measure measure) {
        val recommendation = new MeasureRecommendationItem();
        recommendation.setMeasure(measure);
        recommendation.setCreateTime(System.currentTimeMillis());
        return recommendation;
    }

    static LayoutRecommendationItem createRecommendation(LayoutEntity layoutEntity, boolean isAdd, boolean isAgg) {
        val recommendation = new LayoutRecommendationItem();
        recommendation.setLayout(layoutEntity);
        recommendation.setAggIndex(isAgg);
        recommendation.setAdd(isAdd);
        recommendation.setCreateTime(System.currentTimeMillis());
        val recommendationType = isAdd ? RecommendationType.ADDITION : RecommendationType.REMOVAL;
        recommendation.setRecommendationType(recommendationType);
        return recommendation;
    }

    private Map<Integer, Integer> optimizeModel(NDataModel optimized, NDataModel origin) {
        Preconditions.checkNotNull(optimized, "optimize model not exists");
        Preconditions.checkNotNull(origin, "optimize model not exists");
        logger.info("Semi-Auto-Mode project:{} start to optimize Model:{}", project, optimized.getId());

        val factTable = origin.getRootFactTableAlias() != null ? origin.getRootFactTableAlias()
                : origin.getRootFactTableName().split("\\.")[1];

        val originNamedColumns = origin.getAllNamedColumns();
        val optimizedNamedColumns = optimized.getAllNamedColumns();

        val originComputedColumnDescs = origin.getComputedColumnDescs();
        val optimizedComputedColumnDescs = optimized.getComputedColumnDescs();

        val originAllMeasures = origin.getAllMeasures();
        val optimizedAllMeasures = optimized.getAllMeasures();

        int baseIndex = Math.max(originNamedColumns.get(originNamedColumns.size() - 1).getId(), ID_OFFSET);
        int measureIndex = Math.max(originAllMeasures.get(originAllMeasures.size() - 1).getId(), MEASURE_OFFSET);

        val ccRecommendations = topo(Sets
                .difference(Sets.newHashSet(optimizedComputedColumnDescs), Sets.newHashSet(originComputedColumnDescs))
                .stream().map(OptimizeRecommendationManager::createRecommendation).collect(Collectors.toList()),
                factTable);

        for (val recommendation : ccRecommendations) {
            recommendation.setCcColumnId(++baseIndex);
        }

        val ccRecommendationsMap = ccRecommendations.stream().collect(Collectors.toMap(
                ccSuggestion -> factTable + "." + ccSuggestion.getCc().getColumnName(), ccSuggestion -> ccSuggestion));

        // change cc column to virtual id

        Map<Integer, Integer> translations = Maps.newHashMap();

        val originNamedColumnsMap = originNamedColumns.stream()
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, column -> column));
        val optimizedNamedColumnsMap = optimizedNamedColumns.stream()
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, column -> column));
        Sets.difference(optimizedNamedColumnsMap.keySet(), originNamedColumnsMap.keySet()).forEach(i -> {
            val namedColumn = optimizedNamedColumnsMap.get(i);
            val ccName = optimizedNamedColumnsMap.get(i).getAliasDotColumn();
            val ccVirtualId = ccRecommendationsMap.get(ccName).getCcColumnId();
            val ccRealId = namedColumn.getId();
            translations.put(ccRealId, ccVirtualId);
            namedColumn.setId(ccRecommendationsMap.get(ccName).getCcColumnId());
        });

        val tableManager = NTableMetadataManager.getInstance(getConfig(), project);
        val dataflowManager = NDataflowManager.getInstance(getConfig(), project);
        optimized.init(getConfig(), tableManager.getAllTablesMap(), dataflowManager.listUnderliningDataModels(),
                project);

        val dimensionRecommendations = Lists.<DimensionRecommendationItem> newArrayList();
        originNamedColumnsMap.keySet().forEach(i -> {
            val optimizedColumn = optimizedNamedColumnsMap.get(i);
            val originColumn = originNamedColumnsMap.get(i);
            if (originColumn.isExist() && !originColumn.isDimension() && optimizedColumn.isDimension()) {
                val item = createRecommendation(optimizedColumn);
                item.setDataType(optimized.getColRef(optimizedColumn.getId()).getDatatype());
                dimensionRecommendations.add(item);
            }
        });

        Sets.difference(optimizedNamedColumnsMap.keySet(), originNamedColumnsMap.keySet()).forEach(i -> {
            val optimizedColumn = optimizedNamedColumnsMap.get(i);
            if (optimizedColumn.isDimension()) {
                val item = createRecommendation(optimizedColumn);
                item.setDataType(optimized.getColRef(optimizedColumn.getId()).getDatatype());
                dimensionRecommendations.add(item);
            }
        });

        dimensionRecommendations.sort(Comparator.comparingInt(s -> s.getColumn().getId()));

        List<MeasureRecommendationItem> measureRecommendations = Sets
                .difference(Sets.newHashSet(optimizedAllMeasures), Sets.newHashSet(originAllMeasures)).stream()
                .map(OptimizeRecommendationManager::createRecommendation).collect(Collectors.toList());

        for (val item : measureRecommendations) {
            val virtualMeasureId = ++measureIndex;
            val realMeasureId = item.getMeasure().getId();
            translations.put(realMeasureId, virtualMeasureId);
            item.setMeasureId(virtualMeasureId);
            item.getMeasure().setId(item.getMeasureId());
        }

        val recommendation = copy(getOrCreate(optimized.getId()));
        logOptimizeRecommendation(optimized.getId(), recommendation);
        recommendation.setUuid(optimized.getUuid());
        recommendation.addCCRecommendations(ccRecommendations);
        recommendation.addDimensionRecommendations(dimensionRecommendations);
        recommendation.addMeasureRecommendations(measureRecommendations);
        recommendation.setProject(project);
        updateOptimizeRecommendation(recommendation);

        logOptimizeRecommendation(optimized.getId());
        return translations;
    }

    public interface NOptimizeRecommendationUpdater {
        void modify(OptimizeRecommendation recommendation);
    }

    public OptimizeRecommendation updateOptimizeRecommendation(String recommendationId,
            NOptimizeRecommendationUpdater updater) {
        OptimizeRecommendation cached = getOptimizeRecommendation(recommendationId);
        OptimizeRecommendation copy = copy(cached);
        updater.modify(copy);
        return updateOptimizeRecommendation(copy);
    }

    private OptimizeRecommendation updateOptimizeRecommendation(OptimizeRecommendation optimizeRecommendation) {
        if (optimizeRecommendation.isCachedAndShared())
            throw new IllegalStateException();

        if (optimizeRecommendation.getUuid() == null)
            throw new IllegalArgumentException();

        String name = optimizeRecommendation.getUuid();
        if (!crud.contains(name))
            throw new IllegalArgumentException("IndexPlan '" + name + "' does not exist.");

        return save(optimizeRecommendation);
    }

    private void optimizeIndexPlan(IndexPlan optimized, IndexPlan originIndexPlan, Map<Integer, Integer> translations) {
        Preconditions.checkNotNull(optimized, "optimize index plan not exists");

        val optimizedAllIndexes = optimized.getAllIndexes();

        Preconditions.checkNotNull(originIndexPlan, "index plan " + optimized.getId() + " not exists");
        logger.info("Semi-Auto-Mode project:{} start to optimize IndexPlan:{}", project, optimized.getId());

        val originAllIndexes = originIndexPlan.getAllIndexes();

        val optimizedAllLayouts = optimizedAllIndexes.stream().flatMap(indexEntity -> {
            IndexEntity.IndexIdentifier indexIdentifier = indexEntity.createIndexIdentifier();
            return indexEntity.getLayouts().stream().map(layoutEntity -> new Pair<>(indexIdentifier, layoutEntity));
        }).collect(Collectors.toSet());

        val originAllLayouts = originAllIndexes.stream().flatMap(indexEntity -> {
            IndexEntity.IndexIdentifier indexIdentifier = indexEntity.createIndexIdentifier();
            return indexEntity.getLayouts().stream().map(layoutEntity -> new Pair<>(indexIdentifier, layoutEntity));
        }).collect(Collectors.toSet());

        val delta = Sets.difference(optimizedAllLayouts, originAllLayouts);
        val recommendation = copy(getOrCreate(optimized.getUuid()));
        logOptimizeRecommendation(optimized.getId(), recommendation);

        List<LayoutRecommendationItem> layoutRecommendationItems = delta.stream()
                .collect(Collectors.groupingBy(Pair::getFirst)).entrySet().stream().flatMap(entry -> {
                    val layouts = entry.getValue().stream().map(Pair::getSecond).collect(Collectors.toList());
                    val index = layouts.get(0).getIndex();
                    return layouts.stream().map(layout -> {
                        val layoutCopy = JsonUtil.deepCopyQuietly(layout, LayoutEntity.class);
                        translateLayout(layoutCopy, translations);
                        return createRecommendation(layoutCopy, true, !index.isTableIndex());
                    });
                }).collect(Collectors.toList());
        recommendation.addLayoutRecommendations(layoutRecommendationItems);
        crud.save(recommendation);

        logOptimizeRecommendation(optimized.getId());
    }

    private void translateLayout(LayoutEntity layoutEntity, Map<Integer, Integer> translations) {
        layoutEntity.setColOrder(translateIds(Lists.newArrayList(layoutEntity.getColOrder()), translations));
        layoutEntity.setShardByColumns(translateIds(layoutEntity.getShardByColumns(), translations));
        layoutEntity.setSortByColumns(translateIds(layoutEntity.getSortByColumns(), translations));

    }

    private List<Integer> translateIds(List<Integer> ids, Map<Integer, Integer> translations) {
        for (int i = 0; i < ids.size(); i++) {
            if (translations.containsKey(ids.get(i))) {
                ids.set(i, translations.get(ids.get(i)));
            }
        }
        return ids;
    }

    public void cleanAll(String id) {
        if (getOptimizeRecommendation(id) == null) {
            return;
        }
        logger.info("Semi-Auto-Mode project:{} start to clean all recommendation, id:{}", project, id);
        logOptimizeRecommendation(id);

        updateOptimizeRecommendation(id, recommendation -> {
            recommendation.setCcRecommendations(Lists.newArrayList());
            recommendation.setDimensionRecommendations(Lists.newArrayList());
            recommendation.setMeasureRecommendations(Lists.newArrayList());
            recommendation.setIndexRecommendations(Lists.newArrayList());
            recommendation.setLayoutRecommendations(Lists.newArrayList());
        });
        logOptimizeRecommendation(id);
    }

    public void cleanInEffective(String id) {
        val recommendation = getOptimizeRecommendation(id);

        if (recommendation == null) {
            return;
        }
        logger.info("Semi-Auto-Mode project:{} model:{} start to clean ineffective recommendations.", project, id);

        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        val model = modelManager.getDataModelDesc(id);
        val indexManager = NIndexPlanManager.getInstance(getConfig(), project);
        val indexPlan = indexManager.getIndexPlan(id);

        Preconditions.checkNotNull(model);
        Preconditions.checkNotNull(indexPlan);

        val context = apply(modelManager.copyForWrite(model), indexManager.copy(indexPlan), recommendation);
        update(context);

    }

    public OptimizeRecommendation optimize(NDataModel model, IndexPlan indexPlan) {
        Preconditions.checkNotNull(model);
        Preconditions.checkNotNull(indexPlan);
        logger.info("Semi-Auto-Mode project:{} start to optimize Model:{} and IndexPlan: {}", project, model.getId(),
                indexPlan.getId());

        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        val modelInCache = modelManager.getDataModelDesc(model.getId());
        val indexManager = NIndexPlanManager.getInstance(getConfig(), project);
        val indexPlanInCache = indexManager.getIndexPlan(model.getId());

        Preconditions.checkNotNull(modelInCache);
        Preconditions.checkNotNull(indexPlanInCache);
        val context = apply(modelManager.copyForWrite(modelInCache), indexManager.copy(indexPlanInCache),
                getOrCreate(model.getId()));
        val appliedModel = context.getModel();
        val appliedIndexPlan = context.getIndexPlan();
        val translations = optimizeModel(model, appliedModel);
        optimizeIndexPlan(indexPlan, appliedIndexPlan, translations);
        cleanInEffective(model.getId());
        return getOptimizeRecommendation(model.getId());
    }

    private OptimizeRecommendation getOrCreate(String id) {
        var recommendation = getOptimizeRecommendation(id);
        if (recommendation == null) {
            recommendation = new OptimizeRecommendation();
            recommendation.setProject(project);
            recommendation.setUuid(id);
            crud.save(recommendation);
        }
        return getOptimizeRecommendation(id);
    }

    @AllArgsConstructor
    class NameChangerCCConflictHandler implements ComputedColumnUtil.CCConflictHandler {

        private CCRecommendationItem recommendation;
        private OptimizeContext context;
        private Long lastCCId;

        @Override
        public void handleOnWrongPositionName(NDataModel existingModel, ComputedColumnDesc existingCC,
                ComputedColumnDesc newCC, AliasMapping positionAliasMapping) {
            return;
        }

        @Override
        public void handleOnSameNameDiffExpr(NDataModel existingModel, NDataModel newModel,
                ComputedColumnDesc existingCC, ComputedColumnDesc newCC) {
            if (!recommendation.isAutoChangeName()) {
                throw new PassConflictException(String.format("pass cc %s name conflict", newCC.getColumnName()));
            }
            val newName = newCCName(lastCCId, context.getAllCCNames());
            val oldName = newCC.getColumnName();
            val recommendationItem = context.copyCCRecommendationItem(recommendation.getItemId());
            recommendationItem.getCc().setColumnName(newName);
            context.getNameTranslations().add(new Pair<>(oldName, newName));
        }

        @Override
        public void handleOnWrongPositionExpr(NDataModel existingModel, ComputedColumnDesc existingCC,
                ComputedColumnDesc newCC, AliasMapping positionAliasMapping) {
            return;
        }

        @Override
        public void handleOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
                ComputedColumnDesc newCC) {
            if (!recommendation.isAutoChangeName()) {
                throw new PassConflictException(String.format("pass cc %s %s expression conflict",
                        newCC.getColumnName(), newCC.getInnerExpression()));
            }
            val oldName = newCC.getColumnName();
            val newName = existingCC.getColumnName();
            val recommendationItem = context.copyCCRecommendationItem(recommendation.getItemId());
            recommendationItem.getCc().setColumnName(newName);
            context.getNameTranslations().add(new Pair<>(oldName, newName));
        }
    }

    public NDataModel applyModel(String id) {

        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        if (modelManager.getDataModelDesc(id) == null || indexPlanManager.getIndexPlan(id) == null) {
            return null;
        }
        val recommendation = getOptimizeRecommendation(id);
        if (recommendation == null) {
            return modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        }
        val context = apply(modelManager.copyForWrite(modelManager.getDataModelDesc(recommendation.getId())),
                indexPlanManager.copy(indexPlanManager.getIndexPlan(recommendation.getId())), recommendation);
        return context.getModel();

    }

    private void apply(OptimizeContext context) {
        val recommendation = context.getRecommendation();
        var ccRecommendations = recommendation.getCcRecommendations().stream()
                .sorted(Comparator.comparingLong(RecommendationItem::getItemId)).collect(Collectors.toList());

        val dimensionRecommendations = recommendation.getDimensionRecommendations().stream()
                .sorted(Comparator.comparingLong(RecommendationItem::getItemId)).collect(Collectors.toList());

        val measureRecommendations = recommendation.getMeasureRecommendations().stream()
                .sorted(Comparator.comparingLong(RecommendationItem::getItemId)).collect(Collectors.toList());

        ccRecommendations = topo(ccRecommendations, context.getFactTableName());

        List<Pair<ComputedColumnDesc, NDataModel>> existingCCs = ComputedColumnUtil
                .getExistingCCs(context.getModel().getId(), context.getAllModels());

        Long lastCCId = recommendation.getNextCCRecommendationItemId();

        for (val ccRecommendation : ccRecommendations) {
            checkCCConflictAndApply(ccRecommendation, context, lastCCId, existingCCs);
        }
        applyDimensionRecommendations(dimensionRecommendations, context);
        applyMeasureRecommendations(measureRecommendations, context);

    }

    private void checkCCConflictAndApply(CCRecommendationItem ccRecommendation, OptimizeContext context, Long lastCCId,
            List<Pair<ComputedColumnDesc, NDataModel>> existingCCs) {
        val itemId = ccRecommendation.getItemId();
        ccRecommendation.translate(context);
        int ccCount = context.getModel().getComputedColumnDescs().size();
        for (int i = 0; i < ccCount; i++) {
            val ccInModel = context.getModel().getComputedColumnDescs().get(i);
            var recommendationItem = context.getCCRecommendationItem(itemId);
            if (ComputedColumnUtil.isLiteralSameCCExpr(ccInModel, recommendationItem.getCc())) {
                // need rename or not
                if (!StringUtils.equalsIgnoreCase(ccInModel.getColumnName(),
                        recommendationItem.getCc().getColumnName())) {
                    if (!recommendationItem.isAutoChangeName()) {
                        throw new PassConflictException(String.format("cc %s expression has already defined in model",
                                recommendationItem.getCc().getColumnName()));
                    }
                    context.getNameTranslations()
                            .add(new Pair<>(recommendationItem.getCc().getColumnName(), ccInModel.getColumnName()));
                }
                context.getTranslations().put(recommendationItem.getCcColumnId(), context.getVirtualColumnIdMap()
                        .get(context.getFactTableName() + "." + ccInModel.getColumnName()));
                context.deleteCCRecommendationItem(itemId);
                return;
            } else if (StringUtils.equalsIgnoreCase(ccInModel.getColumnName(),
                    recommendationItem.getCc().getColumnName())) {
                if (!recommendationItem.isAutoChangeName()) {
                    throw new PassConflictException(String.format("cc %s name has already used in model",
                            recommendationItem.getCc().getColumnName()));
                }
                val oldName = recommendationItem.getCc().getColumnName();
                val newName = newCCName(lastCCId, context.getAllCCNames());
                context.getNameTranslations().add(new Pair<>(oldName, newName));

                recommendationItem = context.copyCCRecommendationItem(itemId);
                recommendationItem.getCc().setColumnName(newCCName(lastCCId, context.getAllCCNames()));
            }

        }
        for (Pair<ComputedColumnDesc, NDataModel> pair : existingCCs) {
            NDataModel existingModel = pair.getSecond();
            ComputedColumnDesc existingCC = pair.getFirst();
            ComputedColumnUtil.singleCCConflictCheck(existingModel, context.getModel(), existingCC,
                    context.getCCRecommendationItem(itemId).getCc(),
                    new NameChangerCCConflictHandler(context.getCCRecommendationItem(itemId), context, lastCCId));
        }
        ccRecommendation.checkDependencies(context, false);
        ccRecommendation.apply(context);
    }

    private String newAllName(String name, OptimizeContext context) {
        int i = 0;
        String newName = name;
        while (context.getDimensionColumnNameIdMap().containsKey(newName)) {
            newName = name + "_" + i;
            i++;
        }
        return newName;
    }

    private void applyDimensionRecommendations(List<DimensionRecommendationItem> items, OptimizeContext context) {
        items.forEach(r -> {
            val itemId = r.getItemId();
            val name = r.getColumn().getName();
            if (context.getDimensionColumnNameIdMap().containsKey(name)
                    && !context.getDimensionColumnNameIdMap().get(name).equals(r.getColumn().getId())) {
                if (!r.isAutoChangeName()) {
                    throw new PassConflictException(
                            String.format("dimension all named column %s has already used in model", name));
                }
                val newName = newAllName(r.getColumn().getAliasDotColumn().replace(".", "_"), context);
                val recommendationItem = context.copyDimensionRecommendationItem(itemId);
                recommendationItem.getColumn().setName(newName);
            }
            r.translate(context);
            r.checkDependencies(context, false);
            r.apply(context);
        });
    }

    private void applyMeasureRecommendations(List<MeasureRecommendationItem> items, OptimizeContext context) {
        items.forEach(r -> {
            val itemId = r.getItemId();
            r.translate(context);
            for (val measureInModel : context.getModel().getAllMeasures()) {
                if (measureInModel.isTomb()) {
                    continue;
                }
                val functionInModel = measureInModel.getFunction();

                val measure = context.getMeasureRecommendationItem(itemId).getMeasure();
                if (checkFunctionConflict(functionInModel, measure.getFunction())) {
                    context.getTranslations().put(context.getMeasureRecommendationItem(itemId).getMeasureId(),
                            measureInModel.getId());
                    context.getDeletedMeasureRecommendations().add(itemId);
                    return;
                } else if (measureInModel.getName().equals(measure.getName())) {
                    val copy = context.copyMeasureRecommendationItem(itemId);
                    copy.getMeasure()
                            .setName(newMeasureName(copy.getMeasure().getName(), context.getVirtualMeasures()));
                }
            }
            r.checkDependencies(context, false);
            r.apply(context);
        });
    }

    private boolean checkFunctionConflict(FunctionDesc f1, FunctionDesc f2) {
        if (!f1.getExpression().equals(f2.getExpression())) {
            return false;
        }
        val parameter1 = f1.getParameters().get(0);
        val parameter2 = f2.getParameters().get(0);
        if (!parameter1.getType().equals(parameter2.getType())) {
            return false;
        }
        return parameter1.getValue().equals(parameter2.getValue());
    }

    public IndexPlan applyIndexPlan(String id) {

        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        if (modelManager.getDataModelDesc(id) == null || indexPlanManager.getIndexPlan(id) == null) {
            return null;
        }
        val recommendation = getOptimizeRecommendation(id);
        if (recommendation == null) {
            return indexPlanManager.copy(indexPlanManager.getIndexPlan(id));
        }
        val context = apply(modelManager.copyForWrite(modelManager.getDataModelDesc(recommendation.getId())),
                indexPlanManager.copy(indexPlanManager.getIndexPlan(recommendation.getId())), recommendation);
        return context.getIndexPlan();
    }

    public void logOptimizeRecommendation(String id, OptimizeRecommendation r) {
        if (null == r) {
            logger.info("Semi-Auto-Mode project:{} model:{} print recommendations, recommendations is null!", project,
                    id);
            return;
        }
        logger.info(
                "Semi-Auto-Mode project:{} print recommendations, [Model:{}, CcRecommendations:{}, DimensionRecommendations:{}, MeasureRecommendations:{}, LayoutRecommendations:{}]",
                r.getProject(), r.getId(), r.getCcRecommendations().size(), r.getDimensionRecommendations().size(),
                r.getMeasureRecommendations().size(), r.getLayoutRecommendations().size());
    }

    public void logOptimizeRecommendation(String id) {
        val r = getOptimizeRecommendation(id);
        logOptimizeRecommendation(id, r);
    }

    private void logContextRecommendationItems(String project, NDataModel model,
            OptimizeContext.ContextRecommendationItems items, String type) {
        logger.info(
                "Semi-Auto-Mode project:{} print OptimizeContext type:{} [Model:{}, OriginRecommendations: {}, DeletedRecommendations: {}, ModifiedRecommendations: {}]",
                project, type, model.getId(), items.getOriginRecommendations().size(),
                items.getDeletedRecommendations().size(), items.getModifiedRecommendations().size());
    }

    public void logOptimizeContext(String project, OptimizeContext context) {
        if (null == project || null == context) {
            logger.info("Semi-Auto-Mode print OptimizeContext, project is null:{} OptimizeContext is null:{}",
                    null == project, null == context);
            return;
        }

        logContextRecommendationItems(project, context.getModel(), context.getCcContextRecommendationItems(),
                "CcContextRecommendationItems");
        logContextRecommendationItems(project, context.getModel(), context.getDimensionContextRecommendationItems(),
                "DimensionContextRecommendationItems");
        logContextRecommendationItems(project, context.getModel(), context.getMeasureContextRecommendationItems(),
                "MeasureContextRecommendationItems");
        logContextRecommendationItems(project, context.getModel(), context.getLayoutContextRecommendationItems(),
                "IndexContextRecommendationItems");
        logger.info(
                "Semi-Auto-Mode project:{} print OptimizeContext model [Model:{}, CC:{}, Column:{}, Measure:{}, Index:{}, Layout:{}]",
                project, context.getModel().getId(), context.getModel().getComputedColumnDescs().size(),
                context.getModel().getAllNamedColumns().size(), context.getModel().getAllMeasures().size(),
                context.getIndexPlan().getAllIndexes().size(), context.getIndexPlan().getAllLayouts().size());
    }

    private OptimizeContext apply(NDataModel model, IndexPlan indexPlan, OptimizeRecommendation recommendation) {
        logger.info("Semi-Auto-Mode project:{} start to apply OptimizeContext, [model: {}]", project, model.getId());
        val context = new OptimizeContext(model, indexPlan, recommendation);
        logOptimizeContext(project, context);
        apply(context);
        recommendation.getLayoutRecommendations().stream()
                .sorted(Comparator.comparingLong(RecommendationItem::getItemId)).filter(LayoutRecommendationItem::isAdd)
                .forEach(item -> {
                    item.translate(context);
                    item.checkDependencies(context);
                    item.apply(context);
                });
        logger.info("Semi-Auto-Mode project:{} apply OptimizeContext successfully, [model: {}]", project,
                model.getId());
        logOptimizeContext(project, context);
        return context;
    }

    public void update(OptimizeContext context) {
        update(context, context.getRecommendation().getLastVerifiedTime());
    }

    void update(OptimizeContext context, long lastVerifiedTime) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv() && !UnitOfWork.isAlreadyInTransaction()) {
            logger.warn("cannot update recommendation without transaction.");
            return;
        }
        if (context.getModifiedCCRecommendations().isEmpty() && context.getModifiedDimensionRecommendations().isEmpty()
                && context.getModifiedMeasureRecommendations().isEmpty()
                && context.getModifiedLayoutRecommendations().isEmpty()
                && context.getDeletedCCRecommendations().isEmpty()
                && context.getDeletedDimensionRecommendations().isEmpty()
                && context.getDeletedMeasureRecommendations().isEmpty()
                && context.getDeletedLayoutRecommendations().isEmpty() && context.getTranslations().isEmpty()) {
            return;
        }

        logger.info("Semi-Auto-Mode project:{} start to update recommendation by OptimizeContext, id:{}", project,
                context.getModel().getId());
        if (!context.getTranslations().isEmpty()) {
            context.getRecommendation().getLayoutRecommendations().forEach(item -> item.translate(context));
        }

        val cached = getOptimizeRecommendation(context.getModel().getUuid());
        logOptimizeRecommendation(context.getModel().getId(), cached);
        val copy = copy(cached);
        copy.setCcRecommendations(update(cached.getCcRecommendations(), context.getModifiedCCRecommendations(),
                context.getDeletedCCRecommendations()));
        copy.setDimensionRecommendations(update(cached.getDimensionRecommendations(),
                context.getModifiedDimensionRecommendations(), context.getDeletedDimensionRecommendations()));
        copy.setMeasureRecommendations(update(cached.getMeasureRecommendations(),
                context.getModifiedMeasureRecommendations(), context.getDeletedMeasureRecommendations()));
        copy.setLayoutRecommendations(update(cached.getLayoutRecommendations(),
                context.getModifiedLayoutRecommendations(), context.getDeletedLayoutRecommendations()));

        copy.setLastVerifiedTime(lastVerifiedTime);

        crud.save(copy);
        logOptimizeRecommendation(context.getModel().getId());
    }

    private <T extends RecommendationItem> List<T> update(List<T> items, Map<Long, T> modifies, Set<Long> deletes) {
        val maps = items.stream().collect(Collectors.toMap(RecommendationItem::getItemId, item -> item));
        modifies.forEach(maps::put);
        deletes.forEach(maps::remove);
        val res = new ArrayList<>(maps.values());
        res.sort((i1, i2) -> (int) (i1.getItemId() - i2.getItemId()));
        return res;
    }

    public OptimizeRecommendation copy(OptimizeRecommendation recommendation) {
        return crud.copyBySerialization(recommendation);
    }

    private String newCCName(Long lastCCId, Set<String> existingCCs) {
        var newName = "CC_AUTO_" + (++lastCCId);
        while (existingCCs.contains(newName)) {
            newName = "CC_AUTO_" + (++lastCCId);
        }
        return newName;
    }

    private String newMeasureName(String oldName, Set<String> existingMeasures) {
        int lastIndex = 0;
        var newName = oldName + "_" + (++lastIndex);
        while (existingMeasures.contains(newName)) {
            newName = oldName + "_" + (++lastIndex);
        }
        return newName;
    }

    private static class CCToGraphVisitor extends SqlBasicVisitor {
        private Graph<CCRecommendationItem> graph;
        private Map<String, Graph.Node<CCRecommendationItem>> ccMap;
        private CCRecommendationItem suggestion;
        private String factTable;

        CCToGraphVisitor(Graph<CCRecommendationItem> graph, Map<String, Graph.Node<CCRecommendationItem>> ccMap,
                CCRecommendationItem suggestion, String factTable) {
            this.graph = graph;
            this.ccMap = ccMap;
            this.suggestion = suggestion;
            this.factTable = factTable;
        }

        @Override
        public SqlCall visit(SqlCall call) {
            if (call instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) call;

                for (SqlNode node : basicCall.getOperands()) {
                    node.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlIdentifier visit(SqlIdentifier identifier) {
            String table = factTable;
            String name = identifier.names.get(0);
            if (identifier.names.size() == 2) {
                table = identifier.names.get(0);
                name = identifier.names.get(1);
            }
            if (table.equals(factTable) && ccMap.containsKey(name)) {
                graph.addNode(ccMap.get(name), ccMap.get(suggestion.getCc().getColumnName()));
            }
            return null;
        }
    }

    public static List<CCRecommendationItem> topo(List<CCRecommendationItem> recommendations, String factTable) {
        val ccNamesMap = recommendations.stream()
                .collect(Collectors.toMap(suggestion -> suggestion.getCc().getColumnName(), Graph.Node::new));
        Graph<CCRecommendationItem> graph = new Graph<>();
        recommendations.forEach(ssSuggestion -> {
            val sqlNode = CalciteParser.getExpNode(ssSuggestion.getCc().getExpression());
            val visitor = new CCToGraphVisitor(graph, ccNamesMap, ssSuggestion, factTable);
            if (sqlNode instanceof SqlCall) {
                visitor.visit((SqlCall) sqlNode);
                return;
            } else if (sqlNode instanceof SqlIdentifier) {
                visitor.visit((SqlIdentifier) sqlNode);
                return;
            }
            throw new IllegalCCExpressionException(
                    String.format("illegal cc expression %s ", ssSuggestion.getCc().getColumnName()));
        });

        val topo = new Topo<>(graph);
        val topoList = Lists.newArrayList(topo.getResult()).stream().map(node -> node.val).collect(Collectors.toList());
        val res = recommendations.stream().filter(suggestion -> !topoList.contains(suggestion))
                .collect(Collectors.toList());
        res.addAll(topoList);
        return res;
    }

    public void removeLayouts(String id, Map<Long, GarbageLayoutType> removeLayouts) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(removeLayouts);
        logger.info("Semi-Auto-Mode project:{} start to clean the useless layouts [model:{}, layouts:{}]", project, id,
                removeLayouts.size());
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        var indexPlan = indexPlanManager.getIndexPlan(id);
        Preconditions.checkNotNull(indexPlan);
        indexPlan = applyRemove(indexPlanManager.copy(indexPlan), getOrCreate(id));
        List<LayoutRecommendationItem> indexItems = indexPlan.getAllLayouts().stream()
                .filter(layoutEntity -> removeLayouts.containsKey(layoutEntity.getId()))
                .map(layoutEntity -> new Pair<>(layoutEntity.getIndex(), layoutEntity))
                .collect(Collectors.groupingBy(Pair::getFirst)).entrySet().stream().flatMap(entry -> {
                    val indexEntity = new IndexEntity();
                    val pairs = entry.getValue();
                    val layouts = pairs.stream().map(Pair::getSecond).collect(Collectors.toList());
                    val originIndexEntity = entry.getKey();
                    indexEntity.setId(originIndexEntity.getId());
                    indexEntity.setDimensions(originIndexEntity.getDimensions());
                    indexEntity.setMeasures(originIndexEntity.getMeasures());
                    return layouts.stream().map(layout -> {
                        val layoutCopy = JsonUtil.deepCopyQuietly(layout, LayoutEntity.class);
                        val item = createRecommendation(layoutCopy, false, !originIndexEntity.isTableIndex());
                        item.getExtraInfo().put(REMOVE_REASON, removeLayouts.get(layout.getId()).name());
                        return item;
                    });
                }).collect(Collectors.toList());
        val recommendation = copy(getOrCreate(id));
        logOptimizeRecommendation(id, recommendation);
        recommendation.addLayoutRecommendations(indexItems);
        updateOptimizeRecommendation(recommendation);
        logOptimizeRecommendation(id);
    }

    public IndexPlan applyRemove(IndexPlan indexPlan, OptimizeRecommendation recommendation) {
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        val model = modelManager.copyForWrite(modelManager.getDataModelDesc(indexPlan.getId()));

        val context = new OptimizeContext(model, indexPlan, recommendation);

        val removeLayouts = recommendation.getLayoutRecommendations().stream().filter(item -> !item.isAdd())
                .collect(Collectors.toList());
        removeLayouts.forEach(item -> item.apply(context, false));
        val allWhiteIndexes = indexPlan.getIndexes().stream().filter(indexEntity -> !indexEntity.getLayouts().isEmpty())
                .collect(Collectors.toList());
        indexPlan.setIndexes(allWhiteIndexes);
        return indexPlanManager.copy(indexPlan);
    }

    public void dropOptimizeRecommendation(String id) {
        val recommendation = getOptimizeRecommendation(id);
        if (recommendation == null) {
            return;
        }
        crud.delete(recommendation);
        logger.info("Semi-Auto-Mode project:{} deleted recommendation, id:{}", project, id);
    }

}
