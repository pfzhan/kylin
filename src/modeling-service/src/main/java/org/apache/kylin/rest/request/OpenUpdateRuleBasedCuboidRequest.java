/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.request;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class OpenUpdateRuleBasedCuboidRequest {

    @JsonProperty("project")
    private String project;

    @JsonProperty("model")
    private String modelAlias;

    @JsonProperty("aggregation_groups")
    private List<OpenAggGroupRequest> aggregationGroups;

    @JsonProperty("global_dim_cap")
    private Integer globalDimCap;

    @JsonProperty("restore_deleted_index")
    private boolean restoreDeletedIndex;

    @Data
    public static class OpenAggGroupRequest {

        @JsonProperty("dimensions")
        private String[] dimensions;

        @JsonProperty("measures")
        private String[] measures;

        @JsonProperty("mandatory_dims")
        private String[] mandatoryDims;

        @JsonProperty("hierarchy_dims")
        private String[][] hierarchyDims;

        @JsonProperty("joint_dims")
        private String[][] jointDims;

        @JsonProperty("dim_cap")
        private Integer dimCap;
    }
}
