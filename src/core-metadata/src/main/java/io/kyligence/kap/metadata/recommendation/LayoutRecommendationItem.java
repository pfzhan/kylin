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
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.BitSets;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class LayoutRecommendationItem extends RecommendationItem<LayoutRecommendationItem> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(LayoutRecommendationItem.class);

    public static final String QUERY_HISTORY = "query_history";
    public static final String IMPORTED = "imported";

    @Getter
    @Setter
    @JsonProperty("source")
    private String source = QUERY_HISTORY;

    @Getter
    @Setter
    @JsonProperty("layout_entity")
    private LayoutEntity layout;

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
    @JsonProperty("extra_info")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private Map<String, String> extraInfo = Maps.newHashMap();

    private void checkAddTableIndexDependencies(OptimizeContext context, boolean real) {
        var item = context.getLayoutRecommendationItem(itemId);
        for (int i = 0; i < item.getDimensions().size(); i++) {
            val idColumnMap = context.getVirtualIdColumnMap();
            val id = item.getDimensions().get(i);
            if (!(idColumnMap.containsKey(id)) && !real) {
                context.failIndexRecommendationItem(itemId);
            }
            if (!(idColumnMap.containsKey(id)) || real && OptimizeRecommendationManager.isVirtualColumnId(id)) {
                throw new DependencyLostException(
                        "table index lost dependency: column not exists, you may need pass it first");
            }
        }

        checkLayout(context, real);
    }

    private void checkAddAggIndexDependencies(OptimizeContext context, boolean real) {
        var item = context.getLayoutRecommendationItem(itemId);
        for (int i = 0; i < item.getDimensions().size(); i++) {
            val idColumnMap = context.getVirtualIdColumnMap();
            val id = item.getDimensions().get(i);
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

        for (int i = 0; i < item.getMeasures().size(); i++) {
            val measures = context.getVirtualMeasureIds();
            val id = item.getMeasures().get(i);
            if (!measures.contains(id) && !real) {
                context.failIndexRecommendationItem(itemId);
                return;
            }

            if (!measures.contains(id) || real && OptimizeRecommendationManager.isVirtualMeasureId(id)) {
                throw new DependencyLostException(
                        "agg index lost dependency: measure not exists, you may need pass it first");
            }
        }

        checkLayout(context, real);

    }

    private void checkLayout(OptimizeContext context, boolean real) {
        var item = context.getLayoutRecommendationItem(itemId);
        val identifier = item.createIndexIdentifier();
        if (!context.getAllIndexesMap().containsKey(identifier)) {
            return;
        }
        var index = context.getAllIndexesMap().get(identifier);
        if (!index.getLayouts().contains(getLayout())) {
            return;
        }
        if (real) {
            throw new PassConflictException("cannot add or modify index because index has already added or modified");
        }
        context.failIndexRecommendationItem(itemId);
    }

    private void checkRemoveDependencies(OptimizeContext context, boolean real) {
        var item = context.getLayoutRecommendationItem(itemId);
        val allIndexMap = context.getIndexPlan().getAllIndexesMap();
        val identifier = item.createIndexIdentifier();
        if (!allIndexMap.containsKey(identifier)) {
            if (real) {
                throw new DependencyLostException("cannot remove index because index has already removed.");
            }
            context.getDeletedLayoutRecommendations().add(itemId);
            return;
        }
        if (allIndexMap.get(identifier).getLayouts().contains(item.getLayout())) {
            return;
        }
        if (real) {
            throw new DependencyLostException("cannot remove index because index has already removed.");
        }
        context.failIndexRecommendationItem(itemId);

    }

    @Override
    public void checkDependencies(OptimizeContext context, boolean real) {
        if (context.getDeletedLayoutRecommendations().contains(itemId)) {
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
        if (context.getDeletedLayoutRecommendations().contains(itemId)) {
            return;
        }
        logger.debug(
                "Semi-Auto-Mode project:{} start to apply IndexRecommendationItem, [model:{}, real:{}, type:{}, isAggIndex:{}, indexEntityId:{}, layouts:{}]",
                context.getModel().getProject(), context.getModel().getId(), real, recommendationType, isAggIndex,
                getLayout().getId(), 1);

        if (isAdd()) {
            addLayout(context);
        } else {
            removeLayout(context);
        }

    }

    private void addLayout(OptimizeContext context) {
        var item = context.getLayoutRecommendationItem(itemId);
        val identifier = item.createIndexIdentifier();
        val indexPlan = context.getIndexPlan();
        val layout = item.getLayout();
        if (!context.getWhiteListIndexesMap().containsKey(identifier)) {
            val index = new IndexEntity();
            index.setDimensions(item.getDimensions());
            index.setMeasures(item.getMeasures());
            index.setNextLayoutOffset(1);
            if (context.getAllIndexesMap().get(identifier) != null) {
                index.setId(context.getAllIndexesMap().get(identifier).getId());
                index.setNextLayoutOffset(context.getAllIndexesMap().get(identifier).getNextLayoutOffset() + 1);
            } else {
                index.setId(item.isAggIndex ? indexPlan.getNextAggregationIndexId() : indexPlan.getNextTableIndexId());
            }
            layout.setIndex(index);
            layout.setId(index.getId() + index.getNextLayoutOffset());
            index.setLayouts(Lists.newArrayList(layout));
            index.setNextLayoutOffset(index.getNextLayoutOffset() + 1);
            val indexes = indexPlan.getIndexes();
            indexes.add(index);
            indexPlan.setIndexes(indexes);
            context.getWhiteListIndexesMap().put(identifier, index);

        } else {
            val indexEntity = context.getWhiteListIndexesMap().get(identifier);
            if (indexEntity.getLayouts().contains(layout)) {
                logger.warn("layout " + layout.getColOrder() + " already exists in index.");
                return;
            }
            layout.setId(indexEntity.getId() + indexEntity.getNextLayoutOffset());
            layout.setIndex(indexEntity);
            indexEntity.setNextLayoutOffset(indexEntity.getNextLayoutOffset() + 1);
            indexEntity.getLayouts().add(layout);
        }
    }

    private void removeLayout(OptimizeContext context) {
        var item = context.getLayoutRecommendationItem(itemId);
        val identifier = item.createIndexIdentifier();
        if (context.getAllIndexesMap().containsKey(identifier)) {
            val indexEntity = context.getAllIndexesMap().get(identifier);
            LayoutEntity layout = item.getLayout();
            if (item.isAggIndex() && layout.isManual()) {
                context.getIndexPlan().addRuleBasedBlackList(Lists.newArrayList(layout.getId()));
                return;
            }
            indexEntity.getLayouts().remove(layout);
            context.getIndexPlan().getIndexes().stream()
                    .filter(indexEntityInIndexPlan -> indexEntityInIndexPlan.getId() == indexEntity.getId()).findFirst()
                    .ifPresent(indexEntityInIndexPlan -> indexEntityInIndexPlan.getLayouts().remove(layout));

        } else {
            logger.warn("remove layouts not exists in index plan.");
        }

    }

    @Override
    public LayoutRecommendationItem copy() {
        if (this.isCopy()) {
            return this;
        }
        val res = JsonUtil.deepCopyQuietly(this, LayoutRecommendationItem.class);
        res.setCopy(true);
        return res;
    }

    @Override
    public void translate(OptimizeContext context) {
        if (context.getDeletedLayoutRecommendations().contains(itemId)) {
            return;
        }
        var item = context.getLayoutRecommendationItem(itemId);
        val translations = context.getTranslations();

        val layout = item.getLayout();
        val colOrder = Lists.newArrayList(layout.getColOrder());
        var modified = false;
        for (int j = 0; j < colOrder.size(); j++) {
            if (translations.containsKey(colOrder.get(j))) {
                colOrder.set(j, translations.get(colOrder.get(j)));
                modified = true;
            }
        }
        if (modified) {
            val copyLayout = context.copyIndexRecommendationItem(itemId).getLayout();
            copyLayout.setColOrder(colOrder);
        }
    }

    public IndexEntity.IndexIdentifier createIndexIdentifier() {
        return new IndexEntity.IndexIdentifier(//
                BitSets.valueOf(getDimensions()), //
                BitSets.valueOf(getMeasures()), //
                !isAggIndex()//
        );
    }

    @JsonIgnore
    public List<Integer> getMeasures() {
        return getLayout().getColOrder().stream()
                .filter(i -> (i >= NDataModel.MEASURE_ID_BASE && i < OptimizeRecommendationManager.ID_OFFSET)
                        || (i >= OptimizeRecommendationManager.ID_OFFSET + NDataModel.MEASURE_ID_BASE))
                .collect(Collectors.toList());
    }

    @JsonIgnore
    public List<Integer> getDimensions() {
        return getLayout().getColOrder().stream()
                .filter(i -> (i < NDataModel.MEASURE_ID_BASE) || (i >= OptimizeRecommendationManager.ID_OFFSET
                        && i < OptimizeRecommendationManager.ID_OFFSET + NDataModel.MEASURE_ID_BASE))
                .collect(Collectors.toList());
    }

}
