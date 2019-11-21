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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.MeasureDesc;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class OptRecommendationResponse {
    public static final int PAGING_OFFSET = 0;
    public static final int PAGING_SIZE = 10;

    @JsonProperty("cc_recommendations")
    private List<CCRecommendationItem> ccRecommendations;

    @JsonProperty("dimension_recommendations")
    private List<DimensionRecommendationItem> dimensionRecommendations;

    @JsonProperty("measure_recommendations")
    private List<MeasureRecommendationItem> measureRecommendations;

    @JsonProperty("index_recommendations")
    private List<LayoutRecommendationResponse> indexRecommendations;

    private String modelId;
    private String project;

    public OptRecommendationResponse(OptimizeRecommendation optRecommendation, List<String> sources) {
        this.modelId = optRecommendation.getUuid();
        this.project = optRecommendation.getProject();

        this.ccRecommendations = optRecommendation.getCcRecommendations();
        this.dimensionRecommendations = optRecommendation.getDimensionRecommendations();
        this.measureRecommendations = optRecommendation.getMeasureRecommendations();
        this.indexRecommendations = convertIndexRecommendation(optRecommendation, sources);

    }

    private OptimizeRecommendationManager getOptRecomManager() {
        return OptimizeRecommendationManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    private List<LayoutRecommendationResponse> convertIndexRecommendation(OptimizeRecommendation optimizeRecommendation,
            List<String> sources) {
        val indexRecommendationItems = optimizeRecommendation.getLayoutRecommendations();
        val optimizedModel = getOptRecomManager().applyModel(modelId);
        val idNameMap = optimizedModel.getAllNamedColumns().stream()
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, NDataModel.NamedColumn::getAliasDotColumn));
        idNameMap.putAll(optimizedModel.getAllMeasures().stream()
                .collect(Collectors.toMap(NDataModel.Measure::getId, MeasureDesc::getName)));

        return indexRecommendationItems.stream().map(this::convertToIndexRecommendationResponse).filter(r -> {
            if (CollectionUtils.isEmpty(sources)) {
                return true;
            }
            return sources.contains(r.getSource());
        }).collect(Collectors.toList());

    }

    private LayoutRecommendationResponse convertToIndexRecommendationResponse(LayoutRecommendationItem item) {
        val response = new LayoutRecommendationResponse();
        response.setInfo(item.getExtraInfo());
        response.setItemId(item.getItemId());
        response.setCreatedTime(item.getCreateTime());
        val layout = item.getLayout();
        response.setId(layout.getId());
        response.setColumnsAndMeasuresSize(layout.getColOrder().size());
        if (item.isAdd()) {
            if (item.isAggIndex()) {
                response.setType(LayoutRecommendationResponse.Type.ADD_AGG);
            } else {
                response.setType(LayoutRecommendationResponse.Type.ADD_TABLE);
            }
            response.setSource(item.getSource());
        } else {
            if (item.isAggIndex()) {
                response.setType(LayoutRecommendationResponse.Type.REMOVE_AGG);
            } else {
                response.setType(LayoutRecommendationResponse.Type.REMOVE_TABLE);
            }
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val dataflow = dfManager.getDataflow(modelId);
            response.setDataSize(dataflow.getByteSize(layout.getId()));
            response.setUsage(dataflow.getQueryHitCount(layout.getId()));
        }
        return response;
    }
}
