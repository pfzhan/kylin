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

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;

@Slf4j
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class IndexRecommendationItem implements Serializable, RecommendationItem<IndexRecommendationItem> {
    private static final Logger logger = LoggerFactory.getLogger(IndexRecommendationItem.class);

    @Getter
    @Setter
    @JsonProperty("item_id")
    private long itemId;

    @Getter
    @Setter
    @JsonProperty("index_entity")
    private IndexEntity entity;

    @Getter
    @Setter
    @JsonProperty("is_agg_index")
    private boolean isAggIndex;

    @Getter
    @Setter
    @JsonProperty("is_add")
    private boolean isAdd;

    @Getter
    @Setter
    @JsonProperty("recommendation_type")
    private RecommendationType recommendationType = RecommendationType.ADDITION;

    @Getter
    @Setter
    @JsonIgnore
    private boolean isCopy = false;

    private void checkAddTableIndexDependencies(OptimizeContext context, boolean real) {
        var item = context.getIndexRecommendationItem(itemId);
        for (int i = 0; i < item.entity.getDimensions().size(); i++) {
            val idColumnMap = context.getVirtualIdColumnMap();
            val id = item.entity.getDimensions().get(i);
            if (!(idColumnMap.containsKey(id)) && !real) {
                context.failIndexRecommendationItem(itemId);
            }
            if (!(idColumnMap.containsKey(id)) || real && OptimizeRecommendationManager.isVirtualColumnId(id)) {
                throw new DependencyLostException(
                        "table index lost dependency: column not exists, you may need pass it first");
            }
        }

        checkLayouts(context, real);
    }

    private void checkAddAggIndexDependencies(OptimizeContext context, boolean real) {
        var item = context.getIndexRecommendationItem(itemId);
        for (int i = 0; i < item.entity.getDimensions().size(); i++) {
            val idColumnMap = context.getVirtualIdColumnMap();
            val id = item.entity.getDimensions().get(i);
            if (!(idColumnMap.containsKey(id) && idColumnMap.get(id).isDimension()) && !real) {
                context.failIndexRecommendationItem(itemId);
                return;
            }
            if (!idColumnMap.containsKey(id) || !(idColumnMap.containsKey(id) && idColumnMap.get(id).isDimension())
                    || real && OptimizeRecommendationManager.isVirtualColumnId(id)) {
                throw new DependencyLostException(
                        "agg index lost dependency: dimension not exists, you may need pass it first");
            }
        }

        for (int i = 0; i < item.entity.getMeasures().size(); i++) {
            val measures = context.getVirtualMeasureIds();
            val id = item.entity.getMeasures().get(i);
            if (!measures.contains(id) && !real) {
                context.failIndexRecommendationItem(itemId);
                return;
            }

            if (!measures.contains(id) || real && OptimizeRecommendationManager.isVirtualMeasureId(id)) {
                throw new DependencyLostException(
                        "agg index lost dependency: measure not exists, you may need pass it first");
            }
        }

        checkLayouts(context, real);

    }

    private void checkLayouts(OptimizeContext context, boolean real) {
        var item = context.getIndexRecommendationItem(itemId);
        val identifier = item.getEntity().createIndexIdentifier();
        if (!context.getAllIndexesMap().containsKey(identifier)) {
            return;
        }
        var index = context.getAllIndexesMap().get(identifier);
        val duplicatedLayouts = item.getEntity().getLayouts().stream()
                .filter(layoutEntity -> index.getLayouts().contains(layoutEntity)).collect(Collectors.toSet());
        if (duplicatedLayouts.isEmpty()) {
            return;
        }
        if (real) {
            throw new PassConflictException("cannot add or modify index because index has already added or modified");
        }

        val copy = context.copyIndexRecommendationItem(itemId);
        copy.getEntity().setLayouts(copy.getEntity().getLayouts().stream()
                .filter(layoutEntity -> !duplicatedLayouts.contains(layoutEntity)).collect(Collectors.toList()));
        if (copy.getEntity().getLayouts().isEmpty()) {
            context.failIndexRecommendationItem(itemId);
        }
    }

    private void checkRemoveDependencies(OptimizeContext context, boolean real) {
        var item = context.getIndexRecommendationItem(itemId);
        val allIndexMap = context.getIndexPlan().getAllIndexesMap();
        val identifier = item.getEntity().createIndexIdentifier();
        if (!allIndexMap.containsKey(identifier)) {
            if (real) {
                throw new DependencyLostException("cannot remove index because index has already removed.");
            }
            context.getDeletedIndexRecommendations().add(itemId);
            return;
        }
        val notExistsLayouts = Lists.<LayoutEntity> newArrayList();
        item.getEntity().getLayouts().forEach(layout -> {
            if (!allIndexMap.get(identifier).getLayouts().contains(layout)) {
                notExistsLayouts.add(layout);
            }
        });
        if (notExistsLayouts.isEmpty()) {
            return;
        }
        if (real) {
            throw new DependencyLostException("cannot remove index because index has already removed.");
        }

        val copy = context.copyIndexRecommendationItem(itemId);
        copy.getEntity().getLayouts().removeAll(notExistsLayouts);

    }

    @Override
    public void checkDependencies(OptimizeContext context, boolean real) {
        if (context.getDeletedIndexRecommendations().contains(itemId)) {
            return;
        }
        if (isAdd()) {
            if (isAggIndex()) {
                checkAddAggIndexDependencies(context, real);
            } else {
                checkAddTableIndexDependencies(context, real);
            }
        } else {
            checkRemoveDependencies(context, real);
        }
    }

    @Override
    public void apply(OptimizeContext context, boolean real) {
        if (context.getDeletedIndexRecommendations().contains(itemId)) {
            return;
        }
        log.debug(
                "Semi-Auto-Mode project:{} start to apply IndexRecommendationItem, [model:{}, real:{}, type:{}, isAggIndex:{}, indexEntityId:{}, layouts:{}]",
                context.getModel().getProject(), context.getModel().getId(), real, recommendationType, isAggIndex,
                entity.getId(), entity.getLayouts().size());

        if (isAdd()) {
            addLayouts(context);
        } else {
            removeLayouts(context);
        }

    }

    private void addLayouts(OptimizeContext context) {
        var item = context.getIndexRecommendationItem(itemId);
        val identifier = item.getEntity().createIndexIdentifier();
        val indexPlan = context.getIndexPlan();
        if (!context.getVirtualIndexesMap().containsKey(identifier)) {
            item.entity.setNextLayoutOffset(1);
            val layouts = item.entity.getLayouts();
            if (context.getAllIndexesMap().get(identifier) != null) {
                item.entity.setId(context.getAllIndexesMap().get(identifier).getId());
                item.entity.setNextLayoutOffset(item.entity.getNextLayoutOffset() + 1);
            } else {
                item.entity.setId(
                        item.isAggIndex ? indexPlan.getNextAggregationIndexId() : indexPlan.getNextTableIndexId());
            }
            layouts.forEach(layout -> {
                layout.setId(item.entity.getId() + item.entity.getNextLayoutOffset());
                item.entity.setNextLayoutOffset(item.entity.getNextLayoutOffset() + 1);
            });
            val indexes = indexPlan.getIndexes();
            indexes.add(item.entity);
            indexPlan.setIndexes(indexes);
            context.getVirtualIndexesMap().put(identifier, item.entity);

        } else {
            val indexEntity = context.getVirtualIndexesMap().get(identifier);
            val layouts = item.entity.getLayouts();
            layouts.forEach(layout -> {
                if (indexEntity.getLayouts().contains(layout)) {
                    logger.warn("layout " + layout.getColOrder() + " already exists in index.");
                    return;
                }
                layout.setId(indexEntity.getId() + indexEntity.getNextLayoutOffset());
                indexEntity.setNextLayoutOffset(indexEntity.getNextLayoutOffset() + 1);
                indexEntity.getLayouts().add(layout);
            });
        }
    }

    private void removeLayouts(OptimizeContext context) {
        var item = context.getIndexRecommendationItem(itemId);
        val identifier = item.getEntity().createIndexIdentifier();
        if (context.getAllIndexesMap().containsKey(identifier)) {
            val indexEntity = context.getAllIndexesMap().get(identifier);
            var removeLayouts = item.getEntity().getLayouts().stream()
                    .filter(layoutEntity -> indexEntity.isTableIndex() || layoutEntity.isAuto())
                    .collect(Collectors.toList());
            indexEntity.getLayouts().removeAll(removeLayouts);
            context.getIndexPlan().getIndexes().stream()
                    .filter(indexEntityInIndexPlan -> indexEntityInIndexPlan.getId() == indexEntity.getId()).findFirst()
                    .ifPresent(indexEntityInIndexPlan -> indexEntityInIndexPlan.getLayouts().removeAll(removeLayouts));
            val rule = context.getIndexPlan().getRuleBasedIndex();
            if (rule != null) {
                rule.addBlackListLayouts(item.getEntity().getLayouts().stream()
                        .filter(layoutEntity -> !indexEntity.isTableIndex() && layoutEntity.isManual())
                        .map(LayoutEntity::getId).collect(Collectors.toList()));
            }

        } else {
            logger.warn("remove layouts not exists in index plan.");
        }

    }

    @Override
    public IndexRecommendationItem copy() {
        if (this.isCopy()) {
            return this;
        }
        val res = JsonUtil.deepCopyQuietly(this, IndexRecommendationItem.class);
        res.setCopy(true);
        return res;
    }

    @Override
    public void translate(OptimizeContext context) {
        if (context.getDeletedIndexRecommendations().contains(itemId)) {
            return;
        }
        var item = context.getIndexRecommendationItem(itemId);

        translate(context, i -> i.getEntity().getDimensions());
        translate(context, i -> i.getEntity().getMeasures());
        val translations = context.getTranslations();

        for (int i = 0; i < item.entity.getLayouts().size(); i++) {
            val layout = item.entity.getLayouts().get(i);
            val colOrder = Lists.newArrayList(layout.getColOrder());
            var modified = false;
            for (int j = 0; j < colOrder.size(); j++) {
                if (translations.containsKey(colOrder.get(j))) {
                    colOrder.set(j, translations.get(colOrder.get(j)));
                    modified = true;
                }
            }
            if (modified) {
                val copyLayout = context.copyIndexRecommendationItem(itemId).entity.getLayouts().get(i);
                copyLayout.setColOrder(colOrder);
            }
        }
    }

    private void translate(OptimizeContext context, Function<IndexRecommendationItem, List<Integer>> function) {
        var item = context.getIndexRecommendationItem(itemId);
        val size = function.apply(item).size();
        val translations = context.getTranslations();
        for (int i = 0; i < size; i++) {
            val id = function.apply(item).get(i);
            if (translations.containsKey(id)) {
                item = context.copyIndexRecommendationItem(itemId);
                function.apply(item).set(i, translations.get(id));
            }
        }
    }
}
