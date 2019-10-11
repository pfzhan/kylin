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
import java.util.Objects;
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

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
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

    public static OptimizeRecommendationManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, OptimizeRecommendationManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static OptimizeRecommendationManager newInstance(KylinConfig conf, String project) {

        return new OptimizeRecommendationManager(conf, project);
    }

    public static final int ID_OFFSET = 10000000;
    public static final int MEASURE_OFFSET = 10010000;

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

    public static CCRecommendationItem createRecommendation(ComputedColumnDesc computedColumnDesc) {
        val recommendation = new CCRecommendationItem();
        recommendation.setCc(computedColumnDesc);
        return recommendation;
    }

    public static DimensionRecommendationItem createRecommendation(NDataModel.NamedColumn column) {
        val recommendation = new DimensionRecommendationItem();
        recommendation.setColumn(column);
        return recommendation;
    }

    public static MeasureRecommendationItem createRecommendation(NDataModel.Measure measure) {
        val recommendation = new MeasureRecommendationItem();
        recommendation.setMeasure(measure);
        return recommendation;
    }

    public static IndexRecommendationItem createRecommendation(IndexEntity indexEntity, boolean add) {
        val recommendation = new IndexRecommendationItem();
        recommendation.setEntity(indexEntity);
        recommendation.setAggIndex(!indexEntity.isTableIndex());
        recommendation.setAdd(add);
        val recommendationType = add ? RecommendationType.ADDITION : RecommendationType.REMOVAL;
        recommendation.setRecommendationType(recommendationType);
        return recommendation;
    }

    public OptimizeRecommendation optimize(NDataModel optimized) {
        optimizeModel(optimized);
        return getOptimizeRecommendation(optimized.getId());
    }

    private Map<Integer, Integer> optimizeModel(NDataModel optimized) {
        Preconditions.checkNotNull(optimized, "optimize model not exists");
        val manager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val origin = manager.copyForWrite(manager.getDataModelDesc(optimized.getId()));
        val factTable = origin.getRootFactTableAlias() != null ? origin.getRootFactTableAlias()
                : origin.getRootFactTableName().split("\\.")[1];
        apply(origin, getOptimizeRecommendation(optimized.getId()));

        Preconditions.checkNotNull(origin, "optimize model not exists");

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
        recommendation.setUuid(optimized.getUuid());
        recommendation.addCCRecommendations(ccRecommendations);
        recommendation.addDimensionRecommendations(dimensionRecommendations);
        recommendation.addMeasureRecommendations(measureRecommendations);
        recommendation.setProject(project);
        updateOptimizeRecommendation(recommendation);
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

    private void optimizeIndexPlan(IndexPlan optimized, Map<Integer, Integer> translations) {
        Preconditions.checkNotNull(optimized, "optimize index plan not exists");

        val optimizedAllIndexes = optimized.getAllIndexes();
        val indexManager = NIndexPlanManager.getInstance(config, project);
        val originIndexPlan = indexManager.getIndexPlan(optimized.getUuid());

        val originAllIndexes = originIndexPlan.getAllIndexes();

        val optimizedAllLayouts = optimizedAllIndexes.stream().flatMap(indexEntity -> {
            IndexEntity.IndexIdentifier indexIdentifier = indexEntity.createIndexIdentifier();
            return indexEntity.getLayouts().stream()
                    .map(layoutEntity -> new Pair<>(indexIdentifier, layoutEntity));
        }).collect(Collectors.toSet());

        val originAllLayouts = originAllIndexes.stream().flatMap(indexEntity -> {
            IndexEntity.IndexIdentifier indexIdentifier = indexEntity.createIndexIdentifier();
            return indexEntity.getLayouts().stream()
                    .map(layoutEntity -> new Pair<>(indexIdentifier, layoutEntity));
        }).collect(Collectors.toSet());

        val delta = Sets.difference(optimizedAllLayouts, originAllLayouts);
        val recommendation = copy(getOrCreate(optimized.getUuid()));
        List<IndexRecommendationItem> indexRecommendationItems = delta.stream()
                .collect(Collectors.groupingBy(Pair::getFirst)).entrySet().stream().flatMap(entry -> {
                    val layouts = entry.getValue().stream().map(Pair::getSecond).collect(Collectors.toList());
                    val index = layouts.get(0).getIndex();
                    if (!index.isTableIndex()) {
                        index.setLayouts(layouts);
                        translateIndex(index, translations);
                        return Lists.newArrayList(createRecommendation(index, true)).stream();
                    } else {
                        return layouts.stream().map(layout -> {
                            val indexCopy = JsonUtil.deepCopyQuietly(index, IndexEntity.class);
                            indexCopy.setLayouts(Lists.newArrayList(layout));
                            translateIndex(indexCopy, translations);
                            return createRecommendation(indexCopy, true);
                        });
                    }
                }).collect(Collectors.toList());
        recommendation.addIndexRecommendations(indexRecommendationItems);
        crud.save(recommendation);
    }

    private void translateIndex(IndexEntity indexEntity, Map<Integer, Integer> translations) {
        indexEntity.setDimensions(translateIds(indexEntity.getDimensions(), translations));
        indexEntity.setMeasures(translateIds(indexEntity.getMeasures(), translations));
        indexEntity.getLayouts().forEach(layoutEntity -> {
            layoutEntity.setColOrder(translateIds(Lists.newArrayList(layoutEntity.getColOrder()), translations));
            translateIds(layoutEntity.getShardByColumns(), translations);
            translateIds(layoutEntity.getSortByColumns(), translations);
        });
    }

    private List<Integer> translateIds(List<Integer> ids, Map<Integer, Integer> translations) {
        for (int i = 0; i < ids.size(); i++) {
            if (translations.containsKey(ids.get(i))) {
                ids.set(i, translations.get(ids.get(i)));
            }
        }
        return ids;
    }

    public OptimizeRecommendation optimize(IndexPlan optimized) {
        optimizeIndexPlan(optimized, Maps.newHashMap());
        return getOptimizeRecommendation(optimized.getId());
    }

    public OptimizeRecommendation optimize(NDataModel model, IndexPlan indexPlan) {
        val translations = optimizeModel(model);
        optimizeIndexPlan(indexPlan, translations);
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
        private Integer lastCCId;

        @Override
        public void handleOnWrongPositionName(NDataModel existingModel, ComputedColumnDesc existingCC,
                ComputedColumnDesc newCC, AliasMapping positionAliasMapping) {
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
        return applyModel(copy(getOrCreate(id)));
    }

    public NDataModel applyModel(OptimizeRecommendation recommendation) {
        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        return apply(modelManager.copyForWrite(modelManager.getDataModelDesc(recommendation.getId())), recommendation);
    }

    public NDataModel apply(NDataModel model, OptimizeRecommendation recommendation) {
        if (Objects.isNull(recommendation)) {
            return model;
        }
        val context = new OptimizeContext(model, recommendation);
        apply(context);
        update(context);
        return context.getModel();
    }

    private void apply(OptimizeContext context) {
        val recommendation = context.getRecommendation();
        var ccRecommendations = recommendation.getCcRecommendations();

        val dimensionRecommendations = recommendation.getDimensionRecommendations();

        val measureRecommendations = recommendation.getMeasureRecommendations();

        ccRecommendations = topo(ccRecommendations, context.getFactTableName());

        List<Pair<ComputedColumnDesc, NDataModel>> existingCCs = ComputedColumnUtil
                .getExistingCCs(context.getModel().getId(), context.getAllModels());

        Integer lastCCId = ccRecommendations.isEmpty() ? 0
                : Integer.parseInt(
                        ccRecommendations.get(ccRecommendations.size() - 1).getCc().getColumnName().split("_")[2]);

        for (val ccRecommendation : ccRecommendations) {
            checkCCConflictAndApply(ccRecommendation, context, lastCCId, existingCCs);
        }
        applyDimensionRecommendations(dimensionRecommendations, context);
        applyMeasureRecommendations(measureRecommendations, context);

    }

    private void checkCCConflictAndApply(CCRecommendationItem ccRecommendation, OptimizeContext context,
            Integer lastCCId, List<Pair<ComputedColumnDesc, NDataModel>> existingCCs) {
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
        context.getCCRecommendationItem(itemId).apply(context);
    }

    private String newAllName(String name, OptimizeContext context) {
        int i = 0;
        String newName = name;
        while (context.getVirtualColumnNameIdMap().containsKey(newName)) {
            newName = name + "_" + i;
            i++;
        }
        return newName;
    }

    private void applyDimensionRecommendations(List<DimensionRecommendationItem> items, OptimizeContext context) {
        items.forEach(r -> {
            val itemId = r.getItemId();
            val name = r.getColumn().getName();
            if (context.getVirtualColumnNameIdMap().containsKey(name)
                    && !context.getVirtualColumnNameIdMap().get(name).equals(r.getColumn().getId())) {
                if (!r.isAutoChangeName()) {
                    throw new PassConflictException(
                            String.format("dimension all named column %s has already used in model", name));
                }
                val newName = newAllName(r.getColumn().getAliasDotColumn().replace(".", "_"), context);
                val recommendationItem = context.copyDimensionRecommendationItem(itemId);
                recommendationItem.getColumn().setName(newName);
            }
            r.translate(context);
            context.getDimensionRecommendationItem(itemId).apply(context);
        });
    }

    private void applyMeasureRecommendations(List<MeasureRecommendationItem> items, OptimizeContext context) {
        items.forEach(r -> {
            val itemId = r.getItemId();
            r.translate(context);
            for (val measureInModel : context.getModel().getAllMeasures()) {
                if (context.getDeletedMeasureRecommendations().contains(itemId)) {
                    return;
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
            context.getMeasureRecommendationItem(itemId).apply(context);
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
        return applyIndexPlan(copy(getOrCreate(id)));
    }

    public IndexPlan applyIndexPlan(OptimizeRecommendation recommendation) {
        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        return apply(modelManager.copyForWrite(modelManager.getDataModelDesc(recommendation.getId())),
                indexPlanManager.copy(indexPlanManager.getIndexPlan(recommendation.getId())), recommendation);

    }

    public IndexPlan apply(NDataModel model, IndexPlan indexPlan, OptimizeRecommendation recommendation) {
        if (Objects.isNull(recommendation)) {
            return indexPlan;
        }
        val context = new OptimizeContext(model, indexPlan, recommendation);
        apply(context);
        recommendation.getIndexRecommendations().stream().filter(IndexRecommendationItem::isAdd).forEach(item -> {
            item.checkDependencies(context);
            item.apply(context);
        });
        update(context);
        return context.getIndexPlan();
    }

    public void update(OptimizeContext context) {
        update(context, context.getRecommendation().getLastVerifiedTime());
    }

    void update(OptimizeContext context, long lastVerifiedTime) {
        if (context.getModifiedCCRecommendations().isEmpty() && context.getModifiedDimensionRecommendations().isEmpty()
                && context.getModifiedMeasureRecommendations().isEmpty()
                && context.getModifiedIndexRecommendations().isEmpty()
                && context.getDeletedCCRecommendations().isEmpty()
                && context.getDeletedDimensionRecommendations().isEmpty()
                && context.getDeletedMeasureRecommendations().isEmpty()
                && context.getDeletedIndexRecommendations().isEmpty() && context.getTranslations().isEmpty()) {
            return;
        }

        if (!context.getTranslations().isEmpty()) {
            context.getRecommendation().getIndexRecommendations().forEach(item -> item.translate(context));
        }

        val cached = getOptimizeRecommendation(context.getModel().getUuid());
        val copy = copy(cached);
        copy.setCcRecommendations(update(cached.getCcRecommendations(), context.getModifiedCCRecommendations(),
                context.getDeletedCCRecommendations()));
        copy.setDimensionRecommendations(update(cached.getDimensionRecommendations(),
                context.getModifiedDimensionRecommendations(), context.getDeletedDimensionRecommendations()));
        copy.setMeasureRecommendations(update(cached.getMeasureRecommendations(),
                context.getModifiedMeasureRecommendations(), context.getDeletedMeasureRecommendations()));
        copy.setIndexRecommendations(update(cached.getIndexRecommendations(), context.getModifiedIndexRecommendations(),
                context.getDeletedIndexRecommendations()));

        copy.setLastVerifiedTime(lastVerifiedTime);

        crud.save(copy);

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

    private String newCCName(Integer lastCCId, Set<String> existingCCs) {
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

    public void removeLayouts(String id, Set<Long> removeLayouts) {
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        var indexPlan = indexPlanManager.getIndexPlan(id);
        Preconditions.checkNotNull(indexPlan);
        indexPlan = applyRemove(indexPlanManager.copy(indexPlan), getOrCreate(id));
        List<IndexRecommendationItem> indexItems = indexPlan.getAllLayouts().stream()
                .filter(layoutEntity -> removeLayouts.contains(layoutEntity.getId()))
                .map(layoutEntity -> new Pair<>(layoutEntity.getIndex(), layoutEntity))
                .collect(Collectors.groupingBy(Pair::getFirst)).entrySet().stream().flatMap(entry -> {
                    val indexEntity = new IndexEntity();
                    val pairs = entry.getValue();
                    val layouts = pairs.stream().map(Pair::getSecond).collect(Collectors.toList());
                    val originIndexEntity = entry.getKey();
                    indexEntity.setId(originIndexEntity.getId());
                    indexEntity.setDimensions(originIndexEntity.getDimensions());
                    indexEntity.setMeasures(originIndexEntity.getMeasures());
                    if (!originIndexEntity.isTableIndex()) {
                        indexEntity.setLayouts(layouts);
                        val item = createRecommendation(indexEntity, false);
                        item.setAggIndex(!originIndexEntity.isTableIndex());
                        return Lists.newArrayList(item).stream();
                    } else {
                        return layouts.stream().map(layout -> {
                            val indexCopy = JsonUtil.deepCopyQuietly(indexEntity, IndexEntity.class);
                            indexCopy.setLayouts(Lists.newArrayList(layout));
                            return createRecommendation(indexCopy, false);
                        });

                    }
                }).collect(Collectors.toList());
        val recommendation = copy(getOrCreate(id));
        recommendation.addIndexRecommendations(indexItems);
        updateOptimizeRecommendation(recommendation);
    }

    public IndexPlan applyRemove(IndexPlan indexPlan, OptimizeRecommendation recommendation) {
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        val modelManager = NDataModelManager.getInstance(getConfig(), project);
        val model = modelManager.copyForWrite(modelManager.getDataModelDesc(indexPlan.getId()));

        val context = new OptimizeContext(model, indexPlan, recommendation);

        val removeLayouts = recommendation.getIndexRecommendations().stream().filter(item -> !item.isAdd())
                .collect(Collectors.toList());
        removeLayouts.forEach(item -> item.apply(context, false));
        val allWhiteIndexes = indexPlan.getIndexes().stream().filter(indexEntity -> !indexEntity.getLayouts().isEmpty())
                .collect(Collectors.toList());
        indexPlan.setIndexes(allWhiteIndexes);
        return indexPlanManager.copy(indexPlan);
    }

}
