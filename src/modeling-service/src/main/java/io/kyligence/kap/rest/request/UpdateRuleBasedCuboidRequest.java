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

import org.springframework.beans.BeanUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.insensitive.ProjectInsensitiveRequest;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UpdateRuleBasedCuboidRequest implements ProjectInsensitiveRequest {

    private String project;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("aggregation_groups")
    private List<NAggregationGroup> aggregationGroups;

    @JsonProperty("global_dim_cap")
    private Integer globalDimCap;

    @Builder.Default
    @JsonProperty("scheduler_version")
    private int schedulerVersion = 1;

    @Builder.Default
    @JsonProperty("load_data")
    private boolean isLoadData = true;

    @Builder.Default
    @JsonProperty("restore_deleted_index")
    private boolean restoreDeletedIndex = false;

    public RuleBasedIndex convertToRuleBasedIndex() {
        val newRuleBasedCuboid = new RuleBasedIndex();
        BeanUtils.copyProperties(this, newRuleBasedCuboid);
        newRuleBasedCuboid.adjustMeasures();
        newRuleBasedCuboid.adjustDimensions();
        newRuleBasedCuboid.setGlobalDimCap(globalDimCap);

        return newRuleBasedCuboid;
    }

    public static UpdateRuleBasedCuboidRequest convertToRequest(String project, String modelId, boolean isLoadData,
            RuleBasedIndex ruleBasedIndex) {
        UpdateRuleBasedCuboidRequest updateRuleBasedCuboidRequest = new UpdateRuleBasedCuboidRequest();
        BeanUtils.copyProperties(ruleBasedIndex, updateRuleBasedCuboidRequest);
        updateRuleBasedCuboidRequest.setLoadData(isLoadData);
        updateRuleBasedCuboidRequest.setProject(project);
        updateRuleBasedCuboidRequest.setModelId(modelId);
        return updateRuleBasedCuboidRequest;
    }
}
