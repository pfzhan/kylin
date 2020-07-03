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

package io.kyligence.kap.metadata.recommendation.v2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.model.util.MeasureUtil;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class OptimizeRecommendationV2 extends RootPersistentEntity {

    private static final Logger logger = LoggerFactory.getLogger(OptimizeRecommendationV2.class);

    private static final int CONSTANT_COLUMN_ID = Integer.MAX_VALUE;

    @Getter
    @Setter
    @JsonProperty("raw_ids")
    private List<Integer> rawIds;

    // Ref map. If key >= 0, ref in model else ref in raw item.
    @Getter
    private Map<Integer, ColumnRef> columnRefs = new HashMap<>();
    @Getter
    private Map<Integer, MeasureRef> measureRefs = new HashMap<>();
    @Getter
    private Map<Integer, DimensionRef> dimensionRefs = new HashMap<>();
    @Getter
    private Map<Integer, LayoutRef> layoutRefs = new HashMap<>();

    // cache all raw items.
    @Getter
    private Map<Integer, RawRecItem> rawRecItemMap = new HashMap<>();

    // cache all layout from indexplan.
    @Getter(lazy = true)
    private final List<LayoutEntity> layouts = getAllLayouts();

    // cache model.
    @Getter(lazy = true)
    private final NDataModel model = initModel();

    // cache other models for cc.
    @Getter(lazy = true)
    private final List<NDataModel> otherModels = initOtherModels();

    @Getter
    @Setter
    private List<RootPersistentEntity> dependencies;

    private List<NDataModel> initOtherModels() {
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        return modelManager.listAllModels().stream().filter(m -> !m.isBroken())
                .filter(m -> !m.getId().equals(getModel().getId())).collect(Collectors.toList());
    }

    private NDataModel initModel() {
        return NDataModelManager.getInstance(config, project).getDataModelDesc(getId());
    }

    private List<LayoutEntity> getAllLayouts() {
        return getIndexPlan().getAllLayouts();
    }

    private KylinConfig config;
    private String project;

    public OptimizeRecommendationV2() {
        rawIds = new ArrayList<>();
    }

    private IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(config, project).getIndexPlan(getId());
    }

    private OptimizeRecommendationManagerV2 getOptimizeRecommendationManagerV2() {
        return OptimizeRecommendationManagerV2.getInstance(config, project);
    }

    private static class CCNameCheckerHandler extends ComputedColumnUtil.BasicCCConflictHandler {
        @Override
        public void handleOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
                ComputedColumnDesc newCC) {
            newCC.setColumnName(existingCC.getColumnName());
        }
    }

    public void init(KylinConfig config, String project) {
        logger.debug("Project {} recommendation {} init start", project, getId());

        this.config = config;
        this.project = project;
        NDataModel model = getModel();
        // init model column ref
        Map<String, String> ccNames = model.getComputedColumnDescs().stream()
                .collect(Collectors.toMap(ComputedColumnDesc::getFullName, ComputedColumnDesc::getExpression));
        model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist).forEach(c -> {
            ColumnRef columnRef = new ModelColumnRef(c, model.getEffectiveCols().get(c.getId()).getDatatype(),
                    ccNames.getOrDefault(c.getAliasDotColumn(), c.getAliasDotColumn()));
            columnRefs.put(columnRef.getId(), columnRef);
            if (c.isDimension()) {
                dimensionRefs.put(columnRef.getId(), new DimensionRef(columnRef, c.getId(), true));
            }
        });
        // init model measure ref
        model.getAllMeasures().stream().filter(m -> !m.isTomb()).forEach(measure -> {
            MeasureRef measureRef = new MeasureRef(measure, measure.getId());
            measure.getFunction().getParameters().stream().filter(ParameterDesc::isColumnType).forEach(p -> {
                int id = model.getColumnIdByColumnName(p.getValue());
                measureRef.getColumnRefs().add(columnRefs.get(id));
            });
            measureRef.setExisted(true);
            measureRefs.put(measure.getId(), measureRef);
        });

        for (int rawId : rawIds) {
            init(rawId);
        }

        //generate default cc name
        List<Map.Entry<Integer, ColumnRef>> allCCRefs = getEffectiveCCRef();
        val maxCCIndex = new AtomicInteger(ComputedColumnUtil.getBiggestCCIndex(getModel(), getOtherModels()));
        CCNameCheckerHandler handler = new CCNameCheckerHandler();
        for (Map.Entry<Integer, ColumnRef> entry : allCCRefs) {
            val ccRef = (CCRef) entry.getValue();
            ccRef.getCc().setColumnName(ComputedColumnUtil.CC_NAME_PREFIX + maxCCIndex.incrementAndGet());
            for (NDataModel otherModel : getOtherModels()) {
                for (ComputedColumnDesc existCC : otherModel.getComputedColumnDescs()) {
                    ComputedColumnUtil.singleCCConflictCheck(otherModel, getModel(), existCC, ccRef.getCc(), handler);
                }
            }
        }

        //generate default measure name
        val maxMeasureIndex = new AtomicInteger(MeasureUtil.getBiggestMeasureIndex(getModel()));
        List<Map.Entry<Integer, MeasureRef>> allMeasureRefs = getEffectiveMeasureRef();
        for (Map.Entry<Integer, MeasureRef> entry : allMeasureRefs) {
            val measureRef = entry.getValue();
            measureRef.getMeasure().setName(MeasureUtil.MEASURE_NAME_PREFIX + maxMeasureIndex.incrementAndGet());
        }

        this.setDependencies(calcDependencies());
    }

    @Override
    public List<RootPersistentEntity> calcDependencies() {
        val dataModelManager = NDataModelManager.getInstance(config, project);
        NDataModel dataModelDesc = dataModelManager.getDataModelDesc(getId());
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(getId());
        val res = Lists.<RootPersistentEntity> newArrayList(dataModelDesc != null ? dataModelDesc
                : new MissingRootPersistentEntity(NDataModel.concatResourcePath(getId(), project)));
        res.add(indexPlan != null ? indexPlan
                : new MissingRootPersistentEntity(IndexPlan.concatResourcePath(getId(), project)));
        return res;
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return "/" + project + ResourceStore.MODEL_OPTIMIZE_RECOMMENDATION_V2 + "/" + name
                + MetadataConstants.FILE_SURFIX;
    }

    private void init(int rawId) {
        RawRecItem rawRecItem = getOptimizeRecommendationManagerV2().getRawRecItem(rawId);
        // handle null
        if (rawRecItem == null) {
            logger.warn("Project {} recommendation {} raw item {} is null", project, getId(), rawId);
            columnRefs.put(-rawId, BrokenRefProxy.getProxy(ColumnRef.class, -rawId));
            dimensionRefs.put(-rawId, BrokenRefProxy.getProxy(DimensionRef.class, -rawId));
            measureRefs.put(-rawId, BrokenRefProxy.getProxy(MeasureRef.class, -rawId));
            layoutRefs.put(-rawId, BrokenRefProxy.getProxy(LayoutRef.class, -rawId));
            rawRecItemMap.put(rawId, null);
            return;
        }
        switch (rawRecItem.getType()) {
        case LAYOUT:
            initLayout(rawRecItem);
            break;
        case COMPUTED_COLUMN:
            initCC(rawRecItem);
            break;
        case DIMENSION:
            initDimension(rawRecItem);
            break;
        case MEASURE:
            initMeasure(rawRecItem);
            break;
        default:
            throw new IllegalStateException("id: " + rawId + " type is illegal");
        }
        rawRecItemMap.put(rawId, rawRecItem);
    }

    private void initLayout(RawRecItem layoutRaw) {
        logger.debug("Project {} recommendation {} init layout raw item {}", project, getId(), layoutRaw.getId());
        LayoutRef ref = convertToLayout(layoutRaw);
        layoutRefs.put(-layoutRaw.getId(), ref);
        if (ref.isBroken()) {
            return;
        }
        checkLayoutExists(layoutRaw.getId());
    }

    private void checkLayoutExists(int rawId) {
        // check in model.
        LayoutRef layoutRef = layoutRefs.get(-rawId);
        LayoutEntity layout = JsonUtil.deepCopyQuietly(layoutRef.getLayout(), LayoutEntity.class);
        List<Integer> colOrder = new ArrayList<>(layout.getColOrder().size());
        List<Integer> sortBy = new ArrayList<>(layout.getSortByColumns().size());
        List<Integer> partitionBy = new ArrayList<>(layout.getPartitionByColumns().size());
        List<Integer> shaderBy = new ArrayList<>(layout.getShardByColumns().size());
        boolean containNotExistsColumn = translate(colOrder, layout.getColOrder());
        if (!containNotExistsColumn) {
            translate(sortBy, layout.getSortByColumns());
            translate(shaderBy, layout.getShardByColumns());
            translate(partitionBy, layout.getPartitionByColumns());
            layout.setColOrder(colOrder);
            layout.setShardByColumns(shaderBy);
            layout.setSortByColumns(sortBy);
            layout.setPartitionByColumns(partitionBy);
            val layouts = getLayouts();
            long id = layouts.stream().filter(layoutEntity -> layoutEntity.equals(layout)).map(LayoutEntity::getId)
                    .findFirst().orElse(-1L);
            if (id > 0) {
                layoutRef.setExisted(true);
                return;
            }
        }

        // check in other raw items.
        for (Map.Entry<Integer, LayoutRef> entry : getEffectiveLayoutRef()) {
            if (entry.getKey() == -rawId) {
                continue;
            }
            if (equals(entry.getValue(), layoutRef)) {
                layoutRef.setExisted(true);
                return;
            }
        }

    }

    // Change exist column from raw item to column in model.
    // Return true if there is a not exist column/measure in cols,
    // so we can skip check with layout in index.
    private boolean translate(List<Integer> changed, List<Integer> cols) {
        for (Integer i : cols) {
            if (dimensionRefs.containsKey(i)) {
                val ref = dimensionRefs.get(i);
                if (!ref.isExisted()) {
                    return true;
                }
                changed.add(ref.getId());
            } else {
                val ref = measureRefs.get(i);
                if (!ref.isExisted()) {
                    return true;
                }
                changed.add(ref.getId());
            }

        }
        return false;
    }

    private void logSemanticNotMatch(RawRecItem rawRecItem) {
        logger.warn("Project {} recommendation {} raw item {} semantic version {} is less than model {}", project,
                getId(), rawRecItem.getId(), rawRecItem.getSemanticVersion(), getModel().getSemanticVersion());
    }

    private void logDepLost(RawRecItem rawRecItem, int dep) {
        logger.warn("Project {} recommendation {} raw item {} set deleted, {} not found. {}", project, getId(),
                rawRecItem.getId(), dep, rawRecItem);
    }

    private void logDepDeleted(RawRecItem rawRecItem, int dep) {
        logger.warn("Project {} recommendation {} raw item {} set deleted, {} is deleted. [{}]", project, getId(),
                rawRecItem.getId(), dep, rawRecItem);
    }

    private LayoutRef convertToLayout(RawRecItem layoutRaw) {
        LayoutRef layoutRef = new LayoutRef(RecommendationUtil.getLayout(layoutRaw), -layoutRaw.getId(),
                RecommendationUtil.isAgg(layoutRaw));
        if (getModel().getSemanticVersion() > layoutRaw.getSemanticVersion()) {
            logSemanticNotMatch(layoutRaw);
            return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
        }
        int[] colOrder = layoutRaw.getDependIDs();
        for (int value : colOrder) {
            if (value < 0 && !rawRecItemMap.containsKey(-value)) {
                // dep is not init, init it first.
                init(-value);
            }
            if (dimensionRefs.containsKey(value)) {
                val ref = dimensionRefs.get(value);
                if (ref.isBroken()) {
                    logDepDeleted(layoutRaw, value);
                    return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
                }
                layoutRef.getDimensionRefs().add(ref);
                continue;
            }
            if (measureRefs.containsKey(value)) {
                val ref = measureRefs.get(value);
                if (ref.isBroken()) {
                    logDepDeleted(layoutRaw, value);
                    return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
                }
                layoutRef.getMeasureRefs().add(ref);
                continue;
            }
            // column deleted in model, mark this ref to deleted.
            if (value > 0) {
                logDepLost(layoutRaw, value);
                return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
            }

        }
        return layoutRef;
    }

    private void initCC(RawRecItem rawRecItem) {
        logger.debug("Project {} recommendation {} init cc raw item {}", project, getId(), rawRecItem.getId());
        CCRef ccRef = new CCRef(RecommendationUtil.getCC(rawRecItem), -rawRecItem.getId());
        columnRefs.put(-rawRecItem.getId(), ccRef);
        if (getModel().getSemanticVersion() > rawRecItem.getSemanticVersion()) {
            logSemanticNotMatch(rawRecItem);
            columnRefs.put(ccRef.getId(), BrokenRefProxy.getProxy(CCRef.class, ccRef.getId()));
            return;
        }
        int[] dependId = rawRecItem.getDependIDs();
        for (int id : dependId) {
            if (!columnRefs.containsKey(id)) {
                logDepLost(rawRecItem, id);
                columnRefs.put(ccRef.getId(), BrokenRefProxy.getProxy(CCRef.class, ccRef.getId()));
                return;
            }
            ccRef.getColumnRefs().add(columnRefs.get(id));
        }

        checkCCExist(rawRecItem.getId());
    }

    private void checkCCExist(int rawId) {
        val ref = columnRefs.get(-rawId);
        if (ref.isExisted() || !(ref instanceof CCRef)) {
            return;
        }
        val ccRef = (CCRef) ref;
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
                columnRefs.put(-rawId, columnRefs.get(id.get()));
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
        List<Map.Entry<Integer, ColumnRef>> ccRefs = getEffectiveCCRef();
        for (Map.Entry<Integer, ColumnRef> otherCCRef : ccRefs) {
            if (otherCCRef.getKey() == -rawId) {
                continue;
            }
            id.set(otherCCRef.getKey());
            ComputedColumnUtil.singleCCConflictCheck(getModel(), getModel(), ((CCRef) otherCCRef.getValue()).getCc(),
                    ccRef.getCc(), handler);
            if (existed.get()) {
                return;
            }
        }
    }

    // When a ref is not deleted and does not exist in model.
    Predicate<Map.Entry<Integer, ? extends RecommendationRef>> effectiveFilter = e -> !e.getValue().isBroken()
            && !e.getValue().isExisted() && e.getKey() < 0;

    // When a ref derives from origin model or is effective.
    Predicate<Map.Entry<Integer, ? extends RecommendationRef>> legalFilter = e -> !e.getValue().isBroken()
            && (e.getKey() >= 0 || !e.getValue().isExisted());

    private List<Map.Entry<Integer, ColumnRef>> getEffectiveCCRef() {
        return columnRefs.entrySet().stream().filter(effectiveFilter).collect(Collectors.toList());
    }

    private List<Map.Entry<Integer, MeasureRef>> getEffectiveMeasureRef() {
        return measureRefs.entrySet().stream().filter(effectiveFilter).collect(Collectors.toList());
    }

    private List<Map.Entry<Integer, MeasureRef>> getLegalMeasureRef() {
        return measureRefs.entrySet().stream().filter(legalFilter).collect(Collectors.toList());
    }

    private List<Map.Entry<Integer, DimensionRef>> getEffectiveDimensionRef() {
        return dimensionRefs.entrySet().stream().filter(effectiveFilter).collect(Collectors.toList());
    }

    private List<Map.Entry<Integer, LayoutRef>> getEffectiveLayoutRef() {
        return layoutRefs.entrySet().stream().filter(effectiveFilter).collect(Collectors.toList());
    }

    private void initDimension(RawRecItem rawRecItem) {
        logger.debug("Project {} recommendation {} init dimension raw item {}", project, getId(), rawRecItem.getId());
        int colId = rawRecItem.getDependIDs()[0];
        DimensionRef ref = new DimensionRef(-rawRecItem.getId());
        dimensionRefs.put(-rawRecItem.getId(), ref);
        if (getModel().getSemanticVersion() > rawRecItem.getSemanticVersion()) {
            logSemanticNotMatch(rawRecItem);
            dimensionRefs.put(ref.getId(), BrokenRefProxy.getProxy(DimensionRef.class, ref.getId()));
            return;
        }
        if (colId < 0 && !rawRecItemMap.containsKey(-colId)) {
            init(-colId);
        }
        if (!columnRefs.containsKey(colId)) {
            logDepLost(rawRecItem, colId);
            dimensionRefs.put(ref.getId(), BrokenRefProxy.getProxy(DimensionRef.class, ref.getId()));
            return;
        }
        val colRef = columnRefs.get(colId);
        ref.setColumnRef(colRef);
        if (colRef.isBroken()) {
            logDepDeleted(rawRecItem, colId);
            dimensionRefs.put(ref.getId(), BrokenRefProxy.getProxy(DimensionRef.class, ref.getId()));
            return;
        }
        checkDimensionExist(rawRecItem.getId());
    }

    private void checkDimensionExist(int rawId) {
        DimensionRef ref = dimensionRefs.get(-rawId);
        // check in model.
        if (ref.getColId() >= 0 && dimensionRefs.containsKey(ref.getColId())) {
            ref.setExisted(true);
            dimensionRefs.put(-rawId, dimensionRefs.get(ref.getColId()));
            return;
        }
        // check in other raw items.
        for (Map.Entry<Integer, DimensionRef> entry : getEffectiveDimensionRef()) {
            if (entry.getKey() == -rawId) {
                continue;
            }
            if (entry.getValue().getColId() == ref.getColId()) {
                ref.setExisted(true);
                dimensionRefs.put(entry.getValue().getColId(), dimensionRefs.get(-rawId));
                return;
            }
        }
    }

    private void initMeasure(RawRecItem rawRecItem) {
        logger.debug("Project {} recommendation {} init measure raw item {}", project, getId(), rawRecItem.getId());
        MeasureDesc measure = RecommendationUtil.getMeasure(rawRecItem);
        int[] colOrder = rawRecItem.getDependIDs();
        MeasureRef ref = new MeasureRef(measure, -rawRecItem.getId());
        measureRefs.put(-rawRecItem.getId(), ref);
        if (getModel().getSemanticVersion() > rawRecItem.getSemanticVersion()) {
            logSemanticNotMatch(rawRecItem);
            measureRefs.put(ref.getId(), BrokenRefProxy.getProxy(MeasureRef.class, ref.getId()));
            return;
        }
        for (int value : colOrder) {
            if (value == CONSTANT_COLUMN_ID) {
                continue;
            }

            if (value < 0 && !rawRecItemMap.containsKey(-value)) {
                init(-value);
            }

            if (columnRefs.containsKey(value)) {
                val depRef = columnRefs.get(value);
                if (depRef.isBroken()) {
                    logDepLost(rawRecItem, value);
                    measureRefs.put(ref.getId(), BrokenRefProxy.getProxy(MeasureRef.class, ref.getId()));
                    return;
                }
                ref.getColumnRefs().add(depRef);
                continue;
            }
            // column deleted in model, mark this ref to deleted.
            if (value > 0) {
                logDepLost(rawRecItem, value);
                measureRefs.put(ref.getId(), BrokenRefProxy.getProxy(MeasureRef.class, ref.getId()));
            }

        }
        checkMeasureExist(rawRecItem.getId());
    }

    private void checkMeasureExist(int rawId) {
        val measureRef = measureRefs.get(-rawId);
        for (Map.Entry<Integer, MeasureRef> entry : getLegalMeasureRef()) {
            if (entry.getKey() == -rawId) {
                // pass itself.
                continue;
            }
            if (equals(entry.getValue(), measureRef)) {
                measureRef.setExisted(true);
                measureRefs.put(-rawId, measureRefs.get(entry.getKey()));
                return;
            }
        }

    }

    private boolean equals(LayoutRef ref1, LayoutRef ref2) {
        if (ref1 == null) {
            return ref2 == null;
        }
        if (ref2 == null) {
            return false;
        }

        if (ref1.getDimensionRefs().size() != ref2.getDimensionRefs().size()
                || ref1.getMeasureRefs().size() != ref2.getMeasureRefs().size()) {
            return false;
        }
        if (!refListEquality(ref1.getDimensionRefs(), ref2.getDimensionRefs(), dimensionRefs)) {
            return false;
        }
        if (!refListEquality(ref1.getMeasureRefs(), ref2.getMeasureRefs(), measureRefs)) {
            return false;
        }
        return equalsExtra(ref1, ref2);
    }

    private boolean equalsExtra(LayoutRef ref1, LayoutRef ref2) {
        val layout1 = ref1.getLayout();
        val layout2 = ref2.getLayout();
        return equalsColOrder(layout1.getShardByColumns(), layout2.getShardByColumns())
                && equalsColOrder(layout1.getSortByColumns(), layout2.getSortByColumns());
    }

    private boolean equalsColOrder(List<Integer> colOrder1, List<Integer> colOrder2) {
        if (colOrder1.size() != colOrder2.size()) {
            return false;
        }
        for (int i = 0; i < colOrder1.size(); i++) {
            int col1 = colOrder1.get(i);
            int col2 = colOrder2.get(i);
            if (col1 != col2 && columnRefs.get(col1).getId() != columnRefs.get(col2).getId()) {
                return false;
            }
        }
        return true;
    }

    public boolean equals(MeasureRef m1, MeasureRef m2) {
        if (m1 == null) {
            return m2 == null;
        }
        if (m2 == null) {
            return false;
        }

        if (m1.getDependencies().size() != m2.getDependencies().size()) {
            return false;
        }

        FunctionDesc f1 = m1.getMeasure().getFunction();
        FunctionDesc f2 = m2.getMeasure().getFunction();
        if (!f1.getExpression().equals(f2.getExpression())) {
            return false;
        }

        if (f1.getParameters().size() != f2.getParameters().size()) {
            return false;
        }

        Iterator<ColumnRef> columnRefIterator1 = m1.getDependencies().iterator();
        Iterator<ColumnRef> columnRefIterator2 = m2.getDependencies().iterator();
        for (int i = 0; i < f1.getParameters().size(); i++) {
            ParameterDesc p1 = f1.getParameters().get(i);
            ParameterDesc p2 = f2.getParameters().get(i);
            if (!p1.getType().equals(p2.getType())) {
                return false;
            }
            if (!p1.getType().equals(FunctionDesc.PARAMETER_TYPE_COLUMN)) {
                continue;
            }
            if (!columnRefIterator1.hasNext() || !columnRefIterator2.hasNext()) {
                throw new IllegalStateException("column ref size less than parameter size.");
            }
            ColumnRef ref1 = columnRefIterator1.next();
            ColumnRef ref2 = columnRefIterator2.next();
            if (!refEquality(ref1, ref2, columnRefs)) {
                return false;
            }
        }
        return true;
    }

    private <T extends RecommendationRef> boolean refListEquality(List<T> refs1, List<T> refs2, Map<Integer, T> map) {
        if (refs1.size() != refs2.size()) {
            return false;
        }
        for (int i = 0; i < refs1.size(); i++) {
            T ref1 = refs1.get(i);
            T ref2 = refs2.get(i);
            if (!refEquality(ref1, ref2, map)) {
                return false;
            }
        }
        return true;
    }

    private <T extends RecommendationRef> boolean refEquality(T ref1, T ref2, Map<Integer, T> map) {
        // if ref1 id equals ref2 id, same ref.
        // if ref1 or ref2 exist in model, map contains exist column in model
        return ref1.getId() == ref2.getId() || map.get(ref1.getId()).getId() == map.get(ref2.getId()).getId();
    }

    public List<Integer> validate() {
        List<Integer> res = new ArrayList<>();
        res.addAll(columnRefs.entrySet().stream().filter(entry -> entry.getValue().isBroken()).map(Map.Entry::getKey)
                .collect(Collectors.toList()));
        res.addAll(dimensionRefs.entrySet().stream().filter(entry -> entry.getValue().isBroken()).map(Map.Entry::getKey)
                .collect(Collectors.toList()));
        res.addAll(measureRefs.entrySet().stream().filter(entry -> entry.getValue().isBroken()).map(Map.Entry::getKey)
                .collect(Collectors.toList()));
        res.addAll(layoutRefs.entrySet().stream().filter(entry -> entry.getValue().isBroken()).map(Map.Entry::getKey)
                .collect(Collectors.toList()));
        return res.stream().filter(i -> i < 0).map(i -> -i).collect(Collectors.toList());
    }

    public List<RawRecItem> getAllRawItems(List<Integer> layoutIds) {
        Set<RawRecItem> set = Sets.newHashSet();
        layoutIds.forEach(id -> {
            if (layoutRefs.containsKey(-id)) {
                collect(set, layoutRefs.get(-id));
            }
        });
        return set.stream().sorted(Comparator.comparingLong(RawRecItem::getId)).collect(Collectors.toList());
    }

    public Map<Integer, Integer> getExistMap() {
        val res = Maps.<Integer, Integer> newHashMap();
        columnRefs.forEach((id, ref) -> {
            if (id < 0 && ref.isExisted()) {
                res.put(id, ref.getId());
            }
        });
        dimensionRefs.forEach((id, ref) -> {
            if (id < 0 && ref.isExisted()) {
                res.put(id, ref.getId());
            }
        });
        measureRefs.forEach((id, ref) -> {
            if (id < 0 && ref.isExisted()) {
                res.put(id, ref.getId());
            }
        });
        return res;
    }

    private void collect(Set<RawRecItem> set, RecommendationRef ref) {
        val raw = rawRecItemMap.get(-ref.getId());
        if (set.contains(raw)) {
            return;
        }
        if (!ref.isBroken() && !ref.isExisted()) {
            ref.getDependencies().forEach(dep -> collect(set, dep));
            set.add(raw);
        }
    }

    public Map<Integer, RawRecItem> getEffectiveCCRawRecItems() {
        return getEffectiveRawRecItems(columnRefs);
    }

    private Map<Integer, RawRecItem> getEffectiveRawRecItems(Map<Integer, ? extends RecommendationRef> refs) {
        return refs.entrySet().stream().filter(effectiveFilter)
                .collect(Collectors.toMap(e -> -1 * e.getKey(), e -> rawRecItemMap.get(-1 * e.getKey())));
    }

    public Map<Integer, RawRecItem> getEffectiveDimensionRawRecItems() {
        return getEffectiveRawRecItems(dimensionRefs);
    }

    public Map<Integer, RawRecItem> getEffectiveMeasureRawRecItems() {
        return getEffectiveRawRecItems(measureRefs);
    }

    public Map<Integer, RawRecItem> getEffectiveLayoutRawRecItems() {
        return getEffectiveRawRecItems(layoutRefs);
    }
}
