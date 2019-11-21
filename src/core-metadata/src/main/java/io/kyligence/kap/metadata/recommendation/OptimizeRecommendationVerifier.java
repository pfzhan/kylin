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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.Data;
import lombok.val;

@Data
@Slf4j
public class OptimizeRecommendationVerifier {
    private String id;

    private KylinConfig config;
    private String project;

    private Set<Long> passCCItems;
    private Set<Long> failCCItems;
    private Set<Long> passDimensionItems;
    private Set<Long> failDimensionItems;
    private Set<Long> passMeasureItems;
    private Set<Long> failMeasureItems;
    private Set<Long> passLayoutItems;
    private Set<Long> failLayoutItems;

    public OptimizeRecommendationVerifier(KylinConfig config, String project, String id) {
        this.config = config;
        this.project = project;
        this.id = id;
    }

    private int getSize(Collection items) {
        return null == items ? 0 : items.size();
    }

    public void verify() {
        log.info(
                "Semi-Auto-Mode project:{} start to verify recommendations, [model:{}, passCCItems:{}, failCCItems:{}, passDimensionItems:{}, failDimensionItems:{}, passMeasureItems:{}, failMeasureItems:{}, passIndexItems:{}, failIndexItems:{}]",
                project, id, getSize(passCCItems), getSize(failCCItems), getSize(passDimensionItems),
                getSize(failDimensionItems), getSize(passMeasureItems), getSize(failMeasureItems),
                getSize(passLayoutItems), getSize(failLayoutItems));
        val recommendationManager = OptimizeRecommendationManager.getInstance(config, project);
        val modelManager = NDataModelManager.getInstance(config, project);
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);

        Preconditions.checkNotNull(modelManager.getDataModelDesc(id), "model " + id + " not exists");
        Preconditions.checkNotNull(indexPlanManager.getIndexPlan(id), "index " + id + " not exists");

        recommendationManager.cleanInEffective(id);

        val model = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        val indexPlan = indexPlanManager.copy(indexPlanManager.getIndexPlan(id));
        val recommendation = recommendationManager.getOptimizeRecommendation(id);

        val context = new OptimizeContext(model, indexPlan, recommendation);

        verify(context, recommendation.getCcRecommendations(), passCCItems, failCCItems,
                context.getCcContextRecommendationItems());

        verify(context, recommendation.getDimensionRecommendations(), passDimensionItems, failDimensionItems,
                context.getDimensionContextRecommendationItems());

        verify(context, recommendation.getMeasureRecommendations(), passMeasureItems, failMeasureItems,
                context.getMeasureContextRecommendationItems());

        verify(context, recommendation.getLayoutRecommendations(), passLayoutItems, failLayoutItems,
                context.getLayoutContextRecommendationItems());
        val allNamedColumns = model.getAllNamedColumns().stream()
                .filter(column -> !OptimizeRecommendationManager.isVirtualColumnId(column.getId()))
                .sorted(Comparator.comparingInt(NDataModel.NamedColumn::getId)).collect(Collectors.toList());
        model.setAllNamedColumns(allNamedColumns);

        val realColumns = allNamedColumns.stream().map(NDataModel.NamedColumn::getAliasDotColumn)
                .collect(Collectors.toSet());

        model.setComputedColumnDescs(model.getComputedColumnDescs().stream()
                .filter(computedColumnDesc -> realColumns.contains(
                        (computedColumnDesc.getTableAlias() + "." + computedColumnDesc.getColumnName()).toUpperCase()))
                .collect(Collectors.toList()));

        model.setAllMeasures(model.getAllMeasures().stream()
                .filter(measure -> !OptimizeRecommendationManager.isVirtualMeasureId(measure.getId()))
                .collect(Collectors.toList()));

        modelManager.updateDataModelDesc(model);

        indexPlanManager.updateIndexPlan(indexPlan);

        recommendationManager.update(context, System.currentTimeMillis());

        recommendationManager.cleanInEffective(id);

        recommendationManager.logOptimizeRecommendation(id);
        log.info("Semi-Auto-Mode project:{} verify recommendations successfully, [model:{}]", project, id);
    }

    public void verifyAll() {
        val recommendationManager = OptimizeRecommendationManager.getInstance(config, project);
        val recommendation = recommendationManager.getOptimizeRecommendation(id);

        if (recommendation == null)
            return;

        log.info("Semi-Auto-Mode project:{} start to verify all recommendations, [model:{}]", project, id);

        this.passCCItems = getItemIds(recommendation.getCcRecommendations());
        this.passDimensionItems = getItemIds(recommendation.getDimensionRecommendations());
        this.passMeasureItems = getItemIds(recommendation.getMeasureRecommendations());
        this.passLayoutItems = getItemIds(recommendation.getLayoutRecommendations());

        verify();
    }

    private <T extends RecommendationItem<T>> Set<Long> getItemIds(List<T> recommendationItems) {
        return recommendationItems.stream().map(RecommendationItem::getItemId).collect(Collectors.toSet());
    }

    private <T extends RecommendationItem<T>> void verify(OptimizeContext context, List<T> items, Set<Long> pass,
            Set<Long> fail, OptimizeContext.ContextRecommendationItems<T> contextItems) {
        items.stream().sorted(Comparator.comparingLong(RecommendationItem::getItemId)).forEach(item -> {
            item.translate(context);
            if (pass != null && pass.contains(item.getItemId())) {
                item.checkDependencies(context, true);
                if (contextItems.getDeletedRecommendations().contains(item.getItemId())) {
                    return;
                }
                item.apply(context, true);
                contextItems.getDeletedRecommendations().add(item.getItemId());
            } else if (fail != null && fail.contains(item.getItemId())) {
                contextItems.failRecommendationItem(item.getItemId());
            }
        });
    }
}
