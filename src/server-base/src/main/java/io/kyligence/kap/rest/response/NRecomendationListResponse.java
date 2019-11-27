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

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class NRecomendationListResponse {
    @JsonProperty("origin_model")
    List<NRecomendedDataModelResponse> originModels;
    @JsonProperty("new_model")
    List<NRecomendedDataModelResponse> newModels;

    public NRecomendationListResponse(List<NRecomendedDataModelResponse> originModels,
            List<NRecomendedDataModelResponse> newModels) {
        this.originModels = originModels;
        this.newModels = newModels;
    }

    @Getter
    @Setter
    public static class NRecomendedDataModelResponse extends NDataModel {
        @JsonProperty("sqls")
        private List<String> sqls;
        @JsonProperty("dimensions")
        private List<NamedColumn> dimensions;
        @JsonProperty("recommendation")
        private OptRecommendationResponse recommendationResponse;
        @JsonProperty("index_plan")
        private IndexPlan indices;

        public NRecomendedDataModelResponse(NDataModel other) {
            super(other);
        }

        public NRecomendedDataModelResponse() {
        }
    }
}
