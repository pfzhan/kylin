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

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class OptimizeRecommendation extends RootPersistentEntity {

    @Getter
    @Setter
    @JsonProperty("project")
    private String project;

    @Getter
    @Setter
    @JsonProperty("last_verified_time")
    private long lastVerifiedTime;

    private long modelVersion;

    @Getter
    @Setter
    @JsonProperty("next_cc_recommendation_item_id")
    private long nextCCRecommendationItemId;

    @Getter
    @Setter
    @JsonProperty("next_dimension_recommendation_item_id")
    private long nextDimensionRecommendationItemId;

    @Getter
    @Setter
    @JsonProperty("next_measure_recommendation_item_id")
    private long nextMeasureRecommendationItemId;

    @Getter
    @Setter
    @JsonProperty("next_index_recommendation_item_id")
    private long nextIndexRecommendationItemId;

    @Getter
    @Setter
    @JsonProperty("cc_recommendations")
    private List<CCRecommendationItem> ccRecommendations = Lists.newArrayList();

    @Getter
    @Setter
    @JsonProperty("dimension_recommendations")
    private List<DimensionRecommendationItem> dimensionRecommendations = Lists.newArrayList();

    @Getter
    @Setter
    @JsonProperty("measure_recommendations")
    private List<MeasureRecommendationItem> measureRecommendations = Lists.newArrayList();

    @Getter
    @Setter
    @JsonProperty("index_recommendations")
    private List<IndexRecommendationItem> indexRecommendations = Lists.newArrayList();

    public void addCCRecommendations(List<CCRecommendationItem> ccRecommendations) {
        nextCCRecommendationItemId = addRecommendations(this.ccRecommendations, ccRecommendations,
                nextCCRecommendationItemId);

    }

    public void addDimensionRecommendations(List<DimensionRecommendationItem> dimensionRecommendations) {
        nextDimensionRecommendationItemId = addRecommendations(this.dimensionRecommendations, dimensionRecommendations,
                nextDimensionRecommendationItemId);
    }

    public void addMeasureRecommendations(List<MeasureRecommendationItem> measureRecommendations) {
        nextMeasureRecommendationItemId = addRecommendations(this.measureRecommendations, measureRecommendations,
                nextMeasureRecommendationItemId);
    }

    public void addIndexRecommendations(List<IndexRecommendationItem> indexRecommendationItems) {
        nextIndexRecommendationItemId = addRecommendations(this.indexRecommendations, indexRecommendationItems,
                nextIndexRecommendationItemId);
    }

    private <T extends RecommendationItem<T>> long addRecommendations(List<T> all, List<T> news, long itemId) {
        for (T recommendation : news) {
            recommendation.setItemId(itemId++);
        }
        all.addAll(news);
        return itemId;
    }

}
