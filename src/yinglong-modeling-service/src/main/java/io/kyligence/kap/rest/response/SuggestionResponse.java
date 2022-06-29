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
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class SuggestionResponse {
    @JsonProperty("reused_models")
    List<ModelRecResponse> reusedModels;
    @JsonProperty("new_models")
    List<ModelRecResponse> newModels;
    @JsonProperty("optimal_models")
    List<ModelRecResponse> optimalModels;

    public SuggestionResponse(List<ModelRecResponse> reusedModels, List<ModelRecResponse> newModels) {
        this.reusedModels = reusedModels;
        this.newModels = newModels;
        this.optimalModels = Lists.newArrayList();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    public static class ModelRecResponse extends NDataModel {
        @JsonProperty("rec_items")
        private List<LayoutRecDetailResponse> indexes = Lists.newArrayList();
        @JsonProperty("index_plan")
        private IndexPlan indexPlan;

        public ModelRecResponse(NDataModel dataModel) {
            super(dataModel);
        }
    }
}
