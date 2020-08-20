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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecSelection;
import io.kyligence.kap.metadata.recommendation.util.RawRecUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class OptRecV2 {

    private static final int CONSTANT = Integer.MAX_VALUE;
    private static final String MEASURE_NAME_PREFIX = "MEASURE_AUTO_";

    private final String uuid;
    private final KylinConfig config;
    private final String project;

    private final Map<String, RawRecItem> allModelBasedRecItemMap;
    private final BiMap<String, Integer> uniqueFlagToId = HashBiMap.create();
    private final List<Integer> rawIds = Lists.newArrayList();

    // Ref map. If key >= 0, ref in model else ref in raw item.
    private final Map<Integer, RecommendationRef> columnRefs = Maps.newHashMap();
    private final Map<Integer, RecommendationRef> ccRefs = Maps.newHashMap();
    private final Map<Integer, RecommendationRef> dimensionRefs = Maps.newHashMap();
    private final Map<Integer, RecommendationRef> measureRefs = Maps.newHashMap();
    private final Map<Integer, LayoutRef> layoutRefs = Maps.newHashMap();
    private final Map<Integer, RawRecItem> rawRecItemMap = Maps.newHashMap();
    private final Set<Integer> brokenLayoutRefIds;

    @Getter(lazy = true)
    private final List<LayoutEntity> layouts = getAllLayouts();
    @Getter(lazy = true)
    private final NDataModel model = initModel();
    @Getter(lazy = true)
    private final List<NDataModel> otherModels = initOtherModels();

    public OptRecV2(String project, String uuid) {
        this.config = KylinConfig.getInstanceFromEnv();
        this.uuid = uuid;
        this.project = project;
        // cc cross model?

        allModelBasedRecItemMap = RawRecManager.getInstance(project).queryNonLayoutRecItems(Sets.newHashSet(uuid));
        allModelBasedRecItemMap.forEach((k, recItem) -> uniqueFlagToId.put(k, recItem.getId()));
        initRecommendation();
        brokenLayoutRefIds = filterBrokenLayoutRefs();
    }

    private void initRecommendation() {
        log.debug("Start to initialize recommendation({}/{}}", project, getUuid());

        initModelColumnRefs(getModel());
        initModelMeasureRefs(getModel());
        initLayoutRefs(queryBestLayoutRecItems());

        autoNameForMeasure();

        log.debug("Initialize recommendation({}/{}} successfully.", project, uuid);
    }

    private void autoNameForMeasure() {
        AtomicInteger maxMeasureIndex = new AtomicInteger(getBiggestAutoMeasureIndex(getModel()));
        List<RecommendationRef> allMeasureRefs = getEffectiveRefs(measureRefs);
        for (RecommendationRef entry : allMeasureRefs) {
            MeasureRef measureRef = (MeasureRef) entry;
            String measureName = OptRecV2.MEASURE_NAME_PREFIX + maxMeasureIndex.incrementAndGet();
            measureRef.getMeasure().setName(measureName);
            measureRef.setName(measureName);
            measureRef.setContent(JsonUtil.writeValueAsStringQuietly(measureRef.getMeasure()));
        }
    }

    public int getBiggestAutoMeasureIndex(NDataModel dataModel) {
        int biggest = 0;
        List<String> allAutoMeasureNames = dataModel.getAllMeasures() //
                .stream().map(MeasureDesc::getName) //
                .filter(name -> name.startsWith(MEASURE_NAME_PREFIX)) //
                .collect(Collectors.toList());
        for (String name : allAutoMeasureNames) {
            String idxStr = name.substring(MEASURE_NAME_PREFIX.length());
            if (StringUtils.isEmpty(idxStr)) {
                continue;
            }
            int idx;
            try {
                idx = Integer.parseInt(idxStr);
            } catch (NumberFormatException e) {
                continue;
            }
            if (idx > biggest) {
                biggest = idx;
            }
        }
        return biggest;
    }

    /**
     * Init ModelColumnRefs and DimensionRefs from model
     */
    private void initModelColumnRefs(NDataModel model) {
        List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
        Map<String, String> ccNameToExpressionMap = Maps.newHashMap();
        ccList.forEach(cc -> ccNameToExpressionMap.put(cc.getFullName(), cc.getExpression()));

        for (NDataModel.NamedColumn column : model.getAllNamedColumns()) {
            if (!column.isExist()) {
                continue;
            }

            int id = column.getId();
            String columnName = column.getAliasDotColumn();
            String content = ccNameToExpressionMap.getOrDefault(columnName, columnName);
            String datatype = model.getEffectiveCols().get(column.getId()).getDatatype();
            RecommendationRef columnRef = new ModelColumnRef(column, datatype, content);
            columnRefs.put(id, columnRef);

            if (column.isDimension()) {
                dimensionRefs.put(id, new DimensionRef(columnRef, id, datatype, true));
            }
        }
    }

    /**
     * Init MeasureRefs from model
     */
    private void initModelMeasureRefs(NDataModel model) {
        for (NDataModel.Measure measure : model.getAllMeasures()) {
            if (measure.isTomb()) {
                continue;
            }
            MeasureRef measureRef = new MeasureRef(measure, measure.getId(), true);
            measure.getFunction().getParameters().stream().filter(ParameterDesc::isColumnType).forEach(p -> {
                int id = model.getColumnIdByColumnName(p.getValue());
                measureRef.getDependencies().add(columnRefs.get(id));
            });
            measureRefs.put(measure.getId(), measureRef);
        }
    }

    /**
     * Init LayoutRefs and they derived dependencies(DimensionRef, MeasureRef, CCRef)
     */
    private void initLayoutRefs(List<RawRecItem> bestRecItems) {
        bestRecItems.forEach(rawRecItem -> rawIds.add(rawRecItem.getId()));
        bestRecItems.forEach(rawRecItem -> rawRecItemMap.put(rawRecItem.getId(), rawRecItem));
        bestRecItems.forEach(this::initLayoutRef);
    }

    private List<RawRecItem> queryBestLayoutRecItems() {
        FavoriteRule favoriteRule = FavoriteRuleManager.getInstance(config, project)
                .getByName(FavoriteRule.REC_SELECT_RULE_NAME);
        int topN = Integer.parseInt(
                ((FavoriteRule.Condition) FavoriteRule.getDefaultRule(favoriteRule, FavoriteRule.REC_SELECT_RULE_NAME)
                        .getConds().get(0)).getRightThreshold());
        return RawRecSelection.getInstance().selectBestLayout(topN, uuid, project);
    }

    private void initLayoutRef(RawRecItem rawRecItem) {
        logTranslateInfo(rawRecItem);
        LayoutRef ref = convertToLayoutRef(rawRecItem);
        layoutRefs.put(-rawRecItem.getId(), ref);
        if (ref.isBroken()) {
            return;
        }
        checkLayoutExists(rawRecItem);
    }

    private void checkLayoutExists(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        LayoutRef layoutRef = layoutRefs.get(negRecItemId);
        LayoutEntity layout = JsonUtil.deepCopyQuietly(layoutRef.getLayout(), LayoutEntity.class);
        List<Integer> colOrder = Lists.newArrayList();
        List<Integer> sortColumns = Lists.newArrayList();
        List<Integer> partitionColumns = Lists.newArrayList();
        List<Integer> shardColumns = Lists.newArrayList();
        boolean containNotExistsColumn = translate(colOrder, layout.getColOrder());
        if (!containNotExistsColumn) {
            translate(sortColumns, layout.getSortByColumns());
            translate(shardColumns, layout.getShardByColumns());
            translate(partitionColumns, layout.getPartitionByColumns());
            layout.setColOrder(colOrder);
            layout.setShardByColumns(shardColumns);
            layout.setSortByColumns(sortColumns);
            layout.setPartitionByColumns(partitionColumns);
            long layoutId = getLayouts().stream() //
                    .filter(layoutEntity -> layoutEntity.equals(layout)) //
                    .map(LayoutEntity::getId) //
                    .findFirst().orElse(-1L);
            if (layoutId > 0) {
                logConflictWithRealEntity(recItem, layoutId);
                layoutRef.setExisted(true);
                return;
            }
        }

        // avoid the same LayoutRef
        for (RecommendationRef entry : getEffectiveRefs(layoutRefs)) {
            if (entry.getId() == negRecItemId) {
                continue;
            }
            if (Objects.equals(entry, layoutRef)) {
                logDuplicateRawRecItem(recItem, -entry.getId());
                layoutRef.setExisted(true);
                return;
            }
        }
    }

    // Translate existing column from RawRecItem to column in model.
    // Return true if there is a not exist column/measure in cols,
    // so we can skip check with layout in index.
    private boolean translate(List<Integer> toColIds, List<Integer> fromColIds) {
        for (Integer id : fromColIds) {
            RecommendationRef ref = dimensionRefs.containsKey(id) ? dimensionRefs.get(id) : measureRefs.get(id);
            if (!ref.isExisted()) {
                return true;
            }
            toColIds.add(ref.getId());
        }
        return false;
    }

    private LayoutRef convertToLayoutRef(RawRecItem rawRecItem) {
        int negRecItemId = -rawRecItem.getId();
        NDataModel dataModel = getModel();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            return BrokenRefProxy.getProxy(LayoutRef.class, negRecItemId);
        }

        LayoutEntity layout = RawRecUtil.getLayout(rawRecItem);
        LayoutRef layoutRef = new LayoutRef(layout, negRecItemId, rawRecItem.isAgg());
        for (int dependId : rawRecItem.getDependIDs()) {
            initDependencyRef(dependId, dataModel);

            // normal case: all dependId can be found in dimensionRefs or measureRefs
            if (dimensionRefs.containsKey(dependId) || measureRefs.containsKey(dependId)) {
                RecommendationRef ref = dimensionRefs.containsKey(dependId) //
                        ? dimensionRefs.get(dependId)
                        : measureRefs.get(dependId);
                if (ref.isBroken()) {
                    logDependencyLost(rawRecItem, dependId);
                    return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
                }
                layoutRef.getDependencies().add(ref);
                continue;
            }

            // abnormal case: maybe this column has been deleted in model, mark this ref to deleted.
            if (dependId > 0) {
                logDependencyLost(rawRecItem, dependId);
                return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
            }
        }
        return layoutRef;
    }

    private void initDependencyRef(int dependId, NDataModel dataModel) {
        if (dependId >= 0) {
            log.debug("DependId({}) is derived from model({}/{})", //
                    dependId, getProject(), dataModel.getUuid());
            return;
        }

        int rawRecItemId = -dependId;
        if (rawRecItemMap.containsKey(rawRecItemId)) {
            logRawRecItemHasBeenInitialized(dataModel, rawRecItemId);
            return;
        }

        String uniqueFlag = uniqueFlagToId.inverse().get(rawRecItemId);
        RawRecItem rawRecItem = uniqueFlag == null ? null : allModelBasedRecItemMap.get(uniqueFlag);
        if (rawRecItem == null) {
            logRawRecItemNotFoundError(rawRecItemId);
            ccRefs.put(dependId, BrokenRefProxy.getProxy(CCRef.class, dependId));
            dimensionRefs.put(dependId, BrokenRefProxy.getProxy(DimensionRef.class, dependId));
            measureRefs.put(dependId, BrokenRefProxy.getProxy(MeasureRef.class, dependId));
            rawRecItemMap.put(dependId, null);
            return;
        }
        switch (rawRecItem.getType()) {
        case COMPUTED_COLUMN:
            initCCRef(rawRecItem, dataModel);
            break;
        case DIMENSION:
            initDimensionRef(rawRecItem, dataModel);
            break;
        case MEASURE:
            initMeasureRef(rawRecItem, dataModel);
            break;
        default:
            throw new IllegalStateException("id: " + rawRecItemId + " type is illegal");
        }
        rawRecItemMap.put(rawRecItemId, rawRecItem);
    }

    private void initCCRef(RawRecItem rawRecItem, NDataModel dataModel) {
        logTranslateInfo(rawRecItem);

        int negRecItemId = -rawRecItem.getId();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            ccRefs.put(negRecItemId, BrokenRefProxy.getProxy(CCRef.class, negRecItemId));
            return;
        }

        CCRef ccRef = new CCRef(RawRecUtil.getCC(rawRecItem), negRecItemId);
        int[] dependIds = rawRecItem.getDependIDs();
        for (int dependId : dependIds) {
            TranslatedState state = initDependencyWithState(dependId, ccRef);
            if (state == TranslatedState.BROKEN) {
                logDependencyLost(rawRecItem, dependId);
                ccRefs.put(negRecItemId, BrokenRefProxy.getProxy(CCRef.class, negRecItemId));
                return;
            }
        }
        ccRefs.put(negRecItemId, ccRef);
        checkCCExist(rawRecItem);
    }

    private void checkCCExist(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        RecommendationRef ref = ccRefs.get(negRecItemId);
        if (ref.isExisted() || !(ref instanceof CCRef)) {
            return;
        }
        CCRef ccRef = (CCRef) ref;
        // check in model.
        AtomicInteger id = new AtomicInteger();
        AtomicBoolean existed = new AtomicBoolean();
        ComputedColumnUtil.BasicCCConflictHandler handler = new ComputedColumnUtil.BasicCCConflictHandler() {
            @Override
            public void handleOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
                    ComputedColumnDesc newCC) {
                handleSameExpr();
            }

            @Override
            public void handleOnSameExprSameName(NDataModel existingModel, ComputedColumnDesc existingCC,
                    ComputedColumnDesc newCC) {
                handleSameExpr();
            }

            private void handleSameExpr() {
                ccRef.setExisted(true);
                existed.set(true);
                logCCRewriteInfo(recItem, id.get());
                ccRefs.put(negRecItemId, columnRefs.get(id.get()));
            }
        };
        for (ComputedColumnDesc ccInModel : getModel().getComputedColumnDescs()) {
            id.set(getModel().getColumnIdByColumnName(ccInModel.getFullName()));
            ComputedColumnUtil.singleCCConflictCheck(getModel(), getModel(), ccInModel, ccRef.getCc(), handler);
            if (existed.get()) {
                return;
            }
        }

        // check in other raw items.
        for (RecommendationRef otherCCRef : getEffectiveRefs(ccRefs)) {
            if (otherCCRef.getId() == negRecItemId) {
                continue;
            }
            id.set(otherCCRef.getId());
            final CCRef other = (CCRef) otherCCRef;
            ComputedColumnUtil.singleCCConflictCheck(getModel(), getModel(), other.getCc(), ccRef.getCc(), handler);
            if (existed.get()) {
                return;
            }
        }
    }

    private void initDimensionRef(RawRecItem rawRecItem, NDataModel dataModel) {
        logTranslateInfo(rawRecItem);

        // check semanticVersion
        int negRecItemId = -rawRecItem.getId();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            dimensionRefs.put(negRecItemId, BrokenRefProxy.getProxy(DimensionRef.class, negRecItemId));
            return;
        }

        DimensionRef dimensionRef = new DimensionRef(negRecItemId);
        final int[] dependIDs = rawRecItem.getDependIDs();
        Preconditions.checkArgument(dependIDs.length == 1);
        final TranslatedState state = initDependencyWithState(dependIDs[0], dimensionRef);
        if (state == TranslatedState.BROKEN) {
            logDependencyLost(rawRecItem, dependIDs[0]);
            dimensionRefs.put(negRecItemId, BrokenRefProxy.getProxy(DimensionRef.class, negRecItemId));
            return;
        }
        dimensionRef.init();
        dimensionRefs.put(negRecItemId, dimensionRef);
        checkDimensionExist(rawRecItem);
    }

    private void checkDimensionExist(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        RecommendationRef dimensionRef = dimensionRefs.get(negRecItemId);

        // check two RawRecItems shares same content
        for (RecommendationRef entry : getEffectiveRefs(dimensionRefs)) {
            if (entry.getId() == negRecItemId) {
                // pass itself
                continue;
            }

            // todo: something wrong when renamed
            // if this RawRecItem has been approved, forward to the approved one
            if (Objects.equals(entry, dimensionRef)) {
                logDuplicateRawRecItem(recItem, -entry.getId());
                dimensionRef.setExisted(true);
                dimensionRefs.put(entry.getId(), dimensionRefs.get(entry.getId()));
                return;
            }
        }
    }

    private void initMeasureRef(RawRecItem rawRecItem, NDataModel dataModel) {
        logTranslateInfo(rawRecItem);

        int negRecItemId = -rawRecItem.getId();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
            return;
        }

        // maybe something wrong will happen
        RecommendationRef ref = new MeasureRef(RawRecUtil.getMeasure(rawRecItem), negRecItemId, false);
        for (int value : rawRecItem.getDependIDs()) {
            TranslatedState state = initDependencyWithState(value, ref);
            if (state == TranslatedState.BROKEN) {
                logDependencyLost(rawRecItem, value);
                measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
                return;
            }
        }
        measureRefs.put(negRecItemId, ref);
        checkMeasureExist(rawRecItem);
    }

    private void checkMeasureExist(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        MeasureRef measureRef = (MeasureRef) measureRefs.get(negRecItemId);
        for (RecommendationRef entry : getLegalRefs(measureRefs)) {
            if (entry.getId() == negRecItemId) {
                // pass itself
                continue;
            }

            /* Parameters of measure can only ordinary columns or computed columns,
             * so if dependencies of them are the same, it's equal, then the second
             * measureRef should forward to the first one.
             */
            if (measureRef.isDependenciesIdentical(entry)) {
                logDuplicateRawRecItem(recItem, -entry.getId());
                measureRef.setExisted(true);
                measureRefs.put(negRecItemId, measureRefs.get(entry.getId()));
                return;
            }
        }
    }

    private TranslatedState initDependencyWithState(int dependId, RecommendationRef ref) {
        if (dependId == OptRecV2.CONSTANT) {
            return TranslatedState.CONSTANT;
        }
        NDataModel dataModel = getModel();
        initDependencyRef(dependId, dataModel);

        if (columnRefs.containsKey(dependId)) {
            RecommendationRef e = columnRefs.get(dependId);
            if (e.isBroken()) {
                return TranslatedState.BROKEN;
            }
            ref.getDependencies().add(e);
        } else if (ccRefs.containsKey(dependId)) {
            RecommendationRef e = ccRefs.get(dependId);
            if (e.isBroken()) {
                return TranslatedState.BROKEN;
            }
            ref.getDependencies().add(e);
        } else {
            return TranslatedState.BROKEN;
        }
        return TranslatedState.NORMAL;
    }

    private List<RecommendationRef> getEffectiveRefs(Map<Integer, ? extends RecommendationRef> refMap) {
        List<RecommendationRef> effectiveRefs = Lists.newArrayList();
        refMap.forEach((key, ref) -> {
            if (ref.isEffective()) {
                effectiveRefs.add(ref);
            }
        });
        effectiveRefs.sort(Comparator.comparingInt(RecommendationRef::getId));
        return effectiveRefs;
    }

    private List<RecommendationRef> getLegalRefs(Map<Integer, ? extends RecommendationRef> refMap) {
        List<RecommendationRef> effectiveRefs = Lists.newArrayList();
        refMap.forEach((key, ref) -> {
            if (ref.isLegal()) {
                effectiveRefs.add(ref);
            }
        });
        effectiveRefs.sort(Comparator.comparingInt(RecommendationRef::getId));
        return effectiveRefs;
    }

    private Set<Integer> filterBrokenLayoutRefs() {
        Set<Integer> brokenIds = Sets.newHashSet();
        layoutRefs.forEach((id, ref) -> {
            if (ref.isBroken() && id < 0) {
                brokenIds.add(-id);
            }
        });
        return brokenIds;
    }

    private List<NDataModel> initOtherModels() {
        NDataModelManager modelManager = NDataModelManager.getInstance(Objects.requireNonNull(config), project);
        return modelManager.listAllModels().stream().filter(m -> !m.isBroken())
                .filter(m -> !m.getId().equals(getModel().getId())).collect(Collectors.toList());
    }

    private NDataModel initModel() {
        return NDataModelManager.getInstance(Objects.requireNonNull(config), project).getDataModelDesc(getUuid());
    }

    private List<LayoutEntity> getAllLayouts() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(Objects.requireNonNull(config), project);
        return indexPlanManager.getIndexPlan(getUuid()).getAllLayouts();
    }

    private void logRawRecItemHasBeenInitialized(NDataModel dataModel, int rawRecItemId) {
        log.debug("RawRecItem({}) already initialized for Recommendation({}/{})", //
                rawRecItemId, getProject(), dataModel.getUuid());
    }

    private void logRawRecItemNotFoundError(int rawRecItemId) {
        log.error("RawRecItem({}) is not found in recommendation({}/{})", rawRecItemId, project, getUuid());
    }

    private void logTranslateInfo(RawRecItem recItem) {
        String type;
        switch (recItem.getType()) {
        case MEASURE:
            type = "MeasureRef";
            break;
        case COMPUTED_COLUMN:
            type = "CCRef";
            break;
        case LAYOUT:
            type = "LayoutRef";
            break;
        case DIMENSION:
            type = "DimensionRef";
            break;
        default:
            throw new IllegalArgumentException();
        }
        log.debug("RawRecItem({}) will be translated to {} in Recommendation({}/{})", //
                recItem.getId(), type, project, getUuid());
    }

    private void logCCRewriteInfo(RawRecItem recItem, int rewriteId) {
        log.info("RawRecItem({}) has been rewrite to a NamedColumn({}) for already existing in model({}/{})", //
                recItem.getId(), rewriteId, getProject(), getUuid());
    }

    private void logDependencyLost(RawRecItem rawRecItem, int dependId) {
        log.warn("RawRecItem({}) lost dependency of {} in recommendation({}/{})", //
                rawRecItem.getId(), dependId, getProject(), getUuid());
    }

    private void logSemanticNotMatch(RawRecItem rawRecItem, NDataModel dataModel) {
        log.warn("RawRecItem({}) has an outdated semanticVersion({}) less than {} in recommendation({}/{})",
                rawRecItem.getId(), rawRecItem.getSemanticVersion(), //
                dataModel.getSemanticVersion(), getProject(), getUuid());
    }

    private void logConflictWithRealEntity(RawRecItem recItem, long existingId) {
        log.debug("RawRecItem({}) encounters an existing {}({}) in recommendation({}/{})", //
                recItem.getId(), recItem.getType().name(), existingId, getProject(), getUuid());
    }

    private void logDuplicateRawRecItem(RawRecItem recItem, int anotherRecItemId) {
        log.debug("RawRecItem({}) duplicates with another RawRecItem({}) in recommendation({}/{})", //
                recItem.getId(), anotherRecItemId, getProject(), getUuid());
    }

    private enum TranslatedState {
        CONSTANT, BROKEN, NORMAL, UNDEFINED
    }

}
