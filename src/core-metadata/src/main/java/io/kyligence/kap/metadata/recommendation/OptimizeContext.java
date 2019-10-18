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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.MeasureDesc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.Data;
import lombok.Getter;
import lombok.val;

@Data
public class OptimizeContext {
    private String factTableName;
    private NDataModel model;
    private IndexPlan indexPlan;
    private OptimizeRecommendation recommendation;

    private List<NDataModel> allModels;

    private Set<String> allCCNames;

    // for check update
    private Map<Integer, NDataModel.NamedColumn> virtualIdColumnMap;
    private Map<String, Integer> virtualColumnIdMap;

    private Map<String, Integer> dimensionColumnNameIdMap;

    private Set<String> virtualMeasures;
    private Set<Integer> virtualMeasureIds;

    protected Map<Integer, Integer> translations = Maps.newHashMap();
    private List<Pair<String, String>> nameTranslations = Lists.newArrayList();

    ContextRecommendationItems<CCRecommendationItem> ccContextRecommendationItems;
    ContextRecommendationItems<DimensionRecommendationItem> dimensionContextRecommendationItems;
    ContextRecommendationItems<MeasureRecommendationItem> measureContextRecommendationItems;
    ContextRecommendationItems<IndexRecommendationItem> indexContextRecommendationItems;

    private int originColumnIndex;
    private int originMeasureIndex;

    public OptimizeContext(NDataModel model, OptimizeRecommendation recommendation) {
        this.factTableName = model.getRootFactTableAlias() != null ? model.getRootFactTableAlias()
                : model.getRootFactTableName().split("\\.")[1];
        this.recommendation = recommendation;
        val originCCRecommendations = recommendation.getCcRecommendations().stream()
                .collect(Collectors.toMap(CCRecommendationItem::getItemId, item -> item));
        this.ccContextRecommendationItems = new ContextRecommendationItems<>(originCCRecommendations);

        val originDimensionRecommendations = recommendation.getDimensionRecommendations().stream()
                .collect(Collectors.toMap(DimensionRecommendationItem::getItemId, item -> item));
        this.dimensionContextRecommendationItems = new ContextRecommendationItems<>(originDimensionRecommendations);

        val originMeasureRecommendations = recommendation.getMeasureRecommendations().stream()
                .collect(Collectors.toMap(MeasureRecommendationItem::getItemId, item -> item));
        this.measureContextRecommendationItems = new ContextRecommendationItems<>(originMeasureRecommendations, null);

        val originIndexRecommendations = recommendation.getIndexRecommendations().stream()
                .collect(Collectors.toMap(IndexRecommendationItem::getItemId, item -> item));
        this.indexContextRecommendationItems = new ContextRecommendationItems<>(originIndexRecommendations, null);

        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        this.model = modelManager.copyForWrite(model);
        this.originColumnIndex = this.model.getAllNamedColumns().size();
        this.allModels = modelManager.listAllModels().stream().filter(m -> !m.isBroken()).collect(Collectors.toList());

        this.allCCNames = allModels.stream().flatMap(m -> m.getComputedColumnDescs().stream())
                .map(ComputedColumnDesc::getColumnName).collect(Collectors.toSet());
        this.virtualIdColumnMap = this.model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, m -> m));
        this.virtualColumnIdMap = this.model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getAliasDotColumn, NDataModel.NamedColumn::getId));
        this.dimensionColumnNameIdMap = this.model.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getName, NDataModel.NamedColumn::getId));

        this.virtualMeasures = this.model.getAllMeasures().stream().filter(measure -> !measure.isTomb())
                .map(MeasureDesc::getName).collect(Collectors.toSet());
        this.virtualMeasureIds = this.model.getAllMeasures().stream().filter(measure -> !measure.isTomb())
                .map(NDataModel.Measure::getId).collect(Collectors.toSet());

        this.originMeasureIndex = this.model.getAllMeasures().isEmpty() ? NDataModel.MEASURE_ID_BASE
                : this.model.getAllMeasures().get(this.model.getAllMeasures().size() - 1).getId() + 1;

    }

    // for remove layout
    private Map<IndexEntity.IndexIdentifier, IndexEntity> allIndexesMap;
    // for add layout
    private Map<IndexEntity.IndexIdentifier, IndexEntity> virtualIndexesMap;

    public OptimizeContext(NDataModel model, IndexPlan indexPlan, OptimizeRecommendation recommendation) {
        this(model, recommendation);
        this.indexPlan = indexPlan;
        this.allIndexesMap = indexPlan.getAllIndexesMap();
        this.virtualIndexesMap = indexPlan.getWhiteListIndexesMap();

    }

    public Map<Long, CCRecommendationItem> getModifiedCCRecommendations() {
        return ccContextRecommendationItems.getModifiedRecommendations();
    }

    public Map<Long, DimensionRecommendationItem> getModifiedDimensionRecommendations() {
        return dimensionContextRecommendationItems.getModifiedRecommendations();
    }

    public Map<Long, MeasureRecommendationItem> getModifiedMeasureRecommendations() {
        return measureContextRecommendationItems.getModifiedRecommendations();
    }

    public Map<Long, IndexRecommendationItem> getModifiedIndexRecommendations() {
        return indexContextRecommendationItems.getModifiedRecommendations();
    }

    public Set<Long> getDeletedCCRecommendations() {
        return ccContextRecommendationItems.getDeletedRecommendations();
    }

    public Set<Long> getDeletedDimensionRecommendations() {
        return dimensionContextRecommendationItems.getDeletedRecommendations();
    }

    public Set<Long> getDeletedMeasureRecommendations() {
        return measureContextRecommendationItems.getDeletedRecommendations();
    }

    public Set<Long> getDeletedIndexRecommendations() {
        return indexContextRecommendationItems.getDeletedRecommendations();
    }

    static class ContextRecommendationItems<T extends RecommendationItem<T>> {
        @Getter
        Map<Long, T> modifiedRecommendations = Maps.newHashMap();
        @Getter
        Set<Long> deletedRecommendations = Sets.newHashSet();
        @Getter
        Map<Long, T> originRecommendations;
        BiConsumer<Long, T> actionWhenFail;

        ContextRecommendationItems(Map<Long, T> originRecommendations, BiConsumer<Long, T> actionWhenFail) {
            this.originRecommendations = originRecommendations;
            this.actionWhenFail = actionWhenFail;
        }

        ContextRecommendationItems(Map<Long, T> originRecommendations) {
            this(originRecommendations, null);
        }

        T getRecommendationItem(long id) {
            if (deletedRecommendations.contains(id)) {
                return null;
            }
            if (modifiedRecommendations.containsKey(id)) {
                return modifiedRecommendations.get(id);
            }
            return originRecommendations.get(id);
        }

        void updateRecommendationItem(T updated) {
            modifiedRecommendations.put(updated.getItemId(), updated);
        }

        void deleteRecommendationItem(long id) {
            deletedRecommendations.add(id);
        }

        void failRecommendationItem(long id) {
            if (actionWhenFail != null) {
                actionWhenFail.accept(id, getRecommendationItem(id));
            }
            deleteRecommendationItem(id);
        }

        T copyRecommendationItem(long id) {
            if (deletedRecommendations.contains(id)) {
                throw new RecommendationItemDeletedException("recommendation item " + id + " already deleted");
            }
            if (modifiedRecommendations.containsKey(id)) {
                return modifiedRecommendations.get(id);
            }
            val item = originRecommendations.get(id).copy();
            modifiedRecommendations.put(item.getItemId(), item);
            return item;
        }

    }

    public CCRecommendationItem getCCRecommendationItem(long id) {
        return ccContextRecommendationItems.getRecommendationItem(id);
    }

    public void updateCCRecommendationItem(CCRecommendationItem updated) {
        ccContextRecommendationItems.updateRecommendationItem(updated);
    }

    public void deleteCCRecommendationItem(long id) {
        ccContextRecommendationItems.deleteRecommendationItem(id);
    }

    public void failCCRecommendationItem(long id) {
        ccContextRecommendationItems.failRecommendationItem(id);
    }

    public CCRecommendationItem copyCCRecommendationItem(long id) {
        return ccContextRecommendationItems.copyRecommendationItem(id);
    }

    public DimensionRecommendationItem getDimensionRecommendationItem(long id) {
        return dimensionContextRecommendationItems.getRecommendationItem(id);
    }

    public void updateDimensionRecommendationItem(DimensionRecommendationItem updated) {
        dimensionContextRecommendationItems.updateRecommendationItem(updated);
    }

    public void deleteDimensionRecommendationItem(long id) {
        dimensionContextRecommendationItems.deleteRecommendationItem(id);
    }

    public void failDimensionRecommendationItem(long id) {
        dimensionContextRecommendationItems.failRecommendationItem(id);
    }

    public DimensionRecommendationItem copyDimensionRecommendationItem(long id) {
        return dimensionContextRecommendationItems.copyRecommendationItem(id);
    }

    public MeasureRecommendationItem getMeasureRecommendationItem(long id) {
        return measureContextRecommendationItems.getRecommendationItem(id);
    }

    public void updateMeasureRecommendationItem(MeasureRecommendationItem updated) {
        measureContextRecommendationItems.updateRecommendationItem(updated);
    }

    public void deleteMeasureRecommendationItem(long id) {
        measureContextRecommendationItems.deleteRecommendationItem(id);
    }

    public MeasureRecommendationItem copyMeasureRecommendationItem(long id) {
        return measureContextRecommendationItems.copyRecommendationItem(id);
    }

    public void failMeasureRecommendationItem(long id) {
        measureContextRecommendationItems.failRecommendationItem(id);
    }

    public IndexRecommendationItem getIndexRecommendationItem(long id) {
        return indexContextRecommendationItems.getRecommendationItem(id);
    }

    public void updateIndexRecommendationItem(IndexRecommendationItem updated) {
        indexContextRecommendationItems.updateRecommendationItem(updated);
    }

    public void deleteIndexRecommendationItem(long id) {
        indexContextRecommendationItems.deleteRecommendationItem(id);
    }

    public IndexRecommendationItem copyIndexRecommendationItem(long id) {
        return indexContextRecommendationItems.copyRecommendationItem(id);
    }

    public void failIndexRecommendationItem(long id) {
        indexContextRecommendationItems.failRecommendationItem(id);
    }
}
