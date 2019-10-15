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

package io.kyligence.kap.rest.request;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.rest.response.AggIndexRecommendationResponse;
import io.kyligence.kap.rest.response.TableIndexRecommendationResponse;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ApplyRecommendationsRequest {
    @JsonProperty("project")
    private String project;

    @JsonProperty("model")
    private String modelId;

    @JsonProperty("cc_recommendations")
    private List<CCRecommendationItem> ccRecommendations = Lists.newArrayList();

    @JsonProperty("dimension_recommendations")
    private List<DimensionRecommendationItem> dimensionRecommendations = Lists.newArrayList();

    @JsonProperty("measure_recommendations")
    private List<MeasureRecommendationItem> measureRecommendations = Lists.newArrayList();

    @JsonProperty("agg_index_recommendations")
    private List<AggIndexRecommendationResponse> aggIndexRecommendations = Lists.newArrayList();

    @JsonProperty("table_index_recommendations")
    private List<TableIndexRecommendationResponse> tableIndexRecommendations = Lists.newArrayList();
}
