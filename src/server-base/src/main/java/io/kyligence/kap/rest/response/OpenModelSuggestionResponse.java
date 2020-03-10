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

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@Setter
@Getter
public class OpenModelSuggestionResponse implements Serializable {

    @JsonProperty("models")
    private List<RecommendationsResponse> models;

    @JsonProperty("error_sqls")
    private List<String> errorSqls;

    public static OpenModelSuggestionResponse convert(
            List<NRecomendationListResponse.NRecomendedDataModelResponse> response) {
        OpenModelSuggestionResponse openResponse = new OpenModelSuggestionResponse();
        openResponse.setModels(response.stream().map(RecommendationsResponse::convert).collect(Collectors.toList()));
        return openResponse;
    }

    @Getter
    @Setter
    public static class RecommendationsResponse implements Serializable {
        @JsonProperty("uuid")
        private String uuid;
        @JsonProperty("alias")
        private String alias;
        @JsonProperty("version")
        protected String version;
        @JsonProperty("sqls")
        private List<String> sqls;
        @JsonProperty("dimensions")
        private List<NDataModel.NamedColumn> dimensions;
        @JsonProperty("recommendations")
        private OptRecommendationResponse recommendationResponse;

        public static RecommendationsResponse convert(
                NRecomendationListResponse.NRecomendedDataModelResponse response) {
            RecommendationsResponse recommendationsResponse = new RecommendationsResponse();
            recommendationsResponse.setUuid(response.getUuid());
            recommendationsResponse.setAlias(response.getAlias());
            recommendationsResponse.setVersion(response.getVersion());
            recommendationsResponse.setSqls(response.getSqls());
            recommendationsResponse.setDimensions(response.getDimensions());
            recommendationsResponse.setRecommendationResponse(response.getRecommendationResponse());
            return recommendationsResponse;
        }
    }

}
