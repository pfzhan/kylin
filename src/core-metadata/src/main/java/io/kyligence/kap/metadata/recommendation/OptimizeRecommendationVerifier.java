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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.Data;
import lombok.val;

@Data
public class OptimizeRecommendationVerifier {
    private String id;

    //    private NDataModel model;
    //    private IndexPlan indexPlan;
    //    private OptimizeRecommendation recommendation;

    private KylinConfig config;
    private String project;

    //    private long maxCCItemId;
    //    private long maxDimensionItemId;
    //    private long maxMeasureItemId;
    //    private long maxIndexItemId;

    private Set<Long> passCCItems;
    private Set<Long> failCCItems;
    private Set<Long> passDimensionItems;
    private Set<Long> failDimensionItems;
    private Set<Long> passMeasureItems;
    private Set<Long> failMeasureItems;
    private Set<Long> passIndexItems;
    private Set<Long> failIndexItems;

    public OptimizeRecommendationVerifier(KylinConfig config, String project, String id) {
        this.config = config;
        this.project = project;
        this.id = id;
        //        this.model = model;
        //        this.indexPlan = indexPlan;
        //        this.recommendation = recommendation;
    }

    public void verify() {
        val recommendationManager = OptimizeRecommendationManager.getInstance(config, project);
        val modelManager = NDataModelManager.getInstance(config, project);
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);

        recommendationManager.apply(modelManager.copyForWrite(modelManager.getDataModelDesc(id)),
                recommendationManager.getOptimizeRecommendation(id));
        recommendationManager.apply(modelManager.copyForWrite(modelManager.getDataModelDesc(id)),
                indexPlanManager.copy(indexPlanManager.getIndexPlan(id)),
                recommendationManager.getOptimizeRecommendation(id));

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

        verifyIndexRecommendationItems(context, recommendation.getIndexRecommendations(), passIndexItems,
                failIndexItems, context.getIndexContextRecommendationItems());
        val allNamedColumns = Lists.newArrayList(context.getRealIdColumnMap().values());
        allNamedColumns.sort(Comparator.comparingInt(NDataModel.NamedColumn::getId));
        model.setAllNamedColumns(allNamedColumns);

        model.setComputedColumnDescs(model.getComputedColumnDescs().stream()
                .filter(computedColumnDesc -> context.getRealCCs().contains(computedColumnDesc.getColumnName()))
                .collect(Collectors.toList()));

        model.setAllMeasures(model.getAllMeasures().stream()
                .filter(measure -> context.getRealMeasures().contains(measure.getName())).collect(Collectors.toList()));

        modelManager.updateDataModelDesc(model);

        indexPlanManager.updateIndexPlan(indexPlan);

        recommendationManager.update(context, System.currentTimeMillis());
    }

    private <T extends RecommendationItem<T>> void verify(OptimizeContext context, List<T> items, Set<Long> pass,
            Set<Long> fail, OptimizeContext.ContextRecommendationItems<T> contextItems) {
        verify(context, items, pass, fail, contextItems, item -> item.apply(context, false));
    }

    private void verifyIndexRecommendationItems(OptimizeContext context, List<IndexRecommendationItem> items,
            Set<Long> pass, Set<Long> fail,
            OptimizeContext.ContextRecommendationItems<IndexRecommendationItem> contextItems) {
        verify(context, items, pass, fail, contextItems, null);
    }

    private <T extends RecommendationItem<T>> void verify(OptimizeContext context, List<T> items, Set<Long> pass,
            Set<Long> fail, OptimizeContext.ContextRecommendationItems<T> contextItems, Consumer<T> consumer) {
        items.forEach(item -> {
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
            } else {
                if (contextItems.getDeletedRecommendations().contains(item.getItemId())) {
                    return;
                }
                item.checkDependencies(context, false);
                if (contextItems.getDeletedRecommendations().contains(item.getItemId())) {
                    return;
                }
                if (consumer != null) {
                    consumer.accept(item);
                }
            }
        });
    }
}
