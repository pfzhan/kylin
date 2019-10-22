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

package io.kyligence.kap.rest.response;

import static java.util.stream.Collectors.groupingBy;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.IndexRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.RecommendationType;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;

@Getter
@Setter
public class OptRecommendationResponse {
    public static final int PAGING_OFFSET = 0;
    public static final int PAGING_SIZE = 10;

    @JsonProperty("cc_recommendations")
    private List<CCRecommendationItem> ccRecommendations;

    @JsonProperty("dimension_recommendations")
    private List<DimensionRecommendationItem> dimensionRecommendations;

    @JsonProperty("measure_recommendations")
    private List<MeasureRecommendationItem> measureRecommendations;

    @JsonProperty("agg_index_recommendations")
    private List<AggIndexRecommendationResponse> aggIndexRecommendations;

    @JsonProperty("table_index_recommendations")
    private List<TableIndexRecommendationResponse> tableIndexRecommendations;

    private String modelId;
    private String project;

    public OptRecommendationResponse(OptimizeRecommendation optRecommendation) {
        this.modelId = optRecommendation.getUuid();
        this.project = optRecommendation.getProject();

        this.ccRecommendations = optRecommendation.getCcRecommendations();
        this.dimensionRecommendations = optRecommendation.getDimensionRecommendations();
        this.measureRecommendations = optRecommendation.getMeasureRecommendations();
        val convertedIndexRecomm = convertIndexRecommendation(optRecommendation);
        this.aggIndexRecommendations = convertedIndexRecomm.getFirst();
        this.tableIndexRecommendations = convertedIndexRecomm.getSecond();
    }

    private OptimizeRecommendationManager getOptRecomManager() {
        return OptimizeRecommendationManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    private Pair<List<AggIndexRecommendationResponse>, List<TableIndexRecommendationResponse>> convertIndexRecommendation(
            OptimizeRecommendation optimizeRecommendation) {
        val indexRecommendationItems = optimizeRecommendation.getIndexRecommendations();
        val optimizedModel = getOptRecomManager().applyModel(modelId);

        val aggIndicesRecommendations = new ArrayList<IndexRecommendationItem>();
        val aggIndices = new ArrayList<AggIndexRecommendationResponse>();
        val tableIndices = new ArrayList<TableIndexRecommendationResponse>();

        indexRecommendationItems.forEach(indexRecommendation -> {
            if (indexRecommendation.isAggIndex()) {
                aggIndicesRecommendations.add(indexRecommendation);
            } else {
                tableIndices.addAll(convertToTableIndexResponse(indexRecommendation, optimizedModel));
            }
        });

        aggIndicesRecommendations.stream().collect(groupingBy(index -> index.getEntity().getId()))
                .forEach((indexId, indexRecommItems) -> {
                    val indexEntity = indexRecommItems.get(0).getEntity();
                    boolean isAdd = indexRecommItems.get(0).isAdd();
                    var recommendationType = indexRecommItems.get(0).getRecommendationType();
                    val itemids = Lists.newArrayList(indexRecommItems.get(0).getItemId());

                    for (int i = 1; i < indexRecommItems.size(); i++) {
                        val item = indexRecommItems.get(i);
                        if (item.isAdd() != isAdd) {
                            recommendationType = RecommendationType.MODIFICATION;
                        }

                        indexEntity.getLayouts().addAll(item.getEntity().getLayouts());
                        itemids.add(item.getItemId());
                    }

                    val aggregatedIndex = new AggIndexRecommendationResponse(indexEntity, optimizedModel);
                    aggregatedIndex.setRecommendationType(recommendationType);
                    aggregatedIndex.setItemIds(itemids);
                    aggIndices.add(aggregatedIndex);
                });

        return new Pair<>(aggIndices, tableIndices);
    }

    private List<TableIndexRecommendationResponse> convertToTableIndexResponse(
            IndexRecommendationItem indexRecommendationItem, NDataModel optimizedModel) {
        val tableIndexLayoutsRes = Lists.<TableIndexRecommendationResponse> newArrayList();
        val recommendationType = indexRecommendationItem.isAdd() ? RecommendationType.ADDITION
                : RecommendationType.REMOVAL;
        val indexEntity = indexRecommendationItem.getEntity();
        val itemId = indexRecommendationItem.getItemId();

        indexEntity.getLayouts().forEach(layout -> {
            val tableIndexRes = new TableIndexRecommendationResponse(layout, optimizedModel, null, PAGING_OFFSET,
                    PAGING_SIZE);
            tableIndexRes.setItemId(itemId);
            tableIndexRes.setRecommendationType(recommendationType);
            tableIndexLayoutsRes.add(tableIndexRes);
        });

        return tableIndexLayoutsRes;
    }
}
