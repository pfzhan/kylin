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
package io.kyligence.kap.rest.response;

import java.io.Serializable;

import org.apache.kylin.cube.model.SelectRule;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import lombok.Data;

@Data
public class AggGroupResponse implements Serializable {
    @JsonProperty("includes")
    private String[] includes;
    @JsonProperty("select_rule")
    private AggSelectRule aggSelectRule;

    public AggGroupResponse() {
    }

    public AggGroupResponse(IndexPlan indexPlan, NAggregationGroup aggregationGroup) {
        includes = intArray2StringArray(aggregationGroup.getIncludes(), indexPlan);
        aggSelectRule = new AggSelectRule(indexPlan, aggregationGroup.getSelectRule());
    }

    @Data
    public static class AggSelectRule implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("hierarchy_dims")
        public String[][] hierarchyDims;
        @JsonProperty("mandatory_dims")
        public String[] mandatoryDims;
        @JsonProperty("joint_dims")
        public String[][] jointDims;

        public AggSelectRule() {
        }

        public AggSelectRule(IndexPlan indexPlan, SelectRule selectRule) {
            hierarchyDims = intArray2StringArray(selectRule.getHierarchyDims(), indexPlan);
            mandatoryDims = intArray2StringArray(selectRule.getMandatoryDims(), indexPlan);
            jointDims = intArray2StringArray(selectRule.getJointDims(), indexPlan);
        }
    }

    public static String[] intArray2StringArray(Integer[] ints, IndexPlan indexPlan) {
        int len = ints == null ? 0 : ints.length;
        String[] res;
        if (len > 0) {
            res = new String[len];
            for (int i = 0; i < len; ++i) {
                res[i] = indexPlan.getEffectiveDimCols().get(ints[i]).getIdentity();
            }
        } else {
            res = new String[0];
        }
        return res;
    }

    public static String[][] intArray2StringArray(Integer[][] ints, IndexPlan indexPlan) {
        int p1, p2;
        String[][] res;
        p1 = ints == null ? 0 : ints.length;
        if (p1 > 0) {
            res = new String[p1][];
            for (int i = 0; i < p1; ++i) {
                p2 = ints[i].length;
                res[i] = new String[p2];
                for (int j = 0; j < p2; ++j) {
                    res[i][j] = indexPlan.getEffectiveDimCols().get(ints[i][j]).getIdentity();
                }
            }
        } else {
            res = new String[0][0];
        }
        return res;
    }

}
