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

import java.io.Serializable;

import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.kylin.cube.model.SelectRule;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import lombok.Data;

@Data
public class AggGroupResponse implements Serializable {
    @JsonProperty("includes")
    private String[] includes;
    @JsonProperty("select_rule")
    private AggSelectRule aggSelectRule;

    public AggGroupResponse() {
    }

    public AggGroupResponse(NDataModel dataModel, NAggregationGroup aggregationGroup) {
        includes = intArray2StringArray(aggregationGroup.getIncludes(), dataModel);
        aggSelectRule = new AggSelectRule(dataModel, aggregationGroup.getSelectRule());
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

        public AggSelectRule(NDataModel dataModel, SelectRule selectRule) {
            hierarchyDims = intArray2StringArray(selectRule.getHierarchyDims(), dataModel);
            mandatoryDims = intArray2StringArray(selectRule.getMandatoryDims(), dataModel);
            jointDims = intArray2StringArray(selectRule.getJointDims(), dataModel);
        }
    }

    public static String[] intArray2StringArray(Integer[] ints, NDataModel dataModel) {
        int len = ints == null ? 0 : ints.length;
        String[] res;
        if (len > 0) {
            res = new String[len];
            for (int i = 0; i < len; ++i) {
                res[i] = dataModel.getEffectiveDimensions().get(ints[i]).getIdentity();
            }
        } else {
            res = new String[0];
        }
        return res;
    }

    public static String[][] intArray2StringArray(Integer[][] ints, NDataModel dataModel) {
        int p1, p2;
        String[][] res;
        p1 = ints == null ? 0 : ints.length;
        if (p1 > 0) {
            res = new String[p1][];
            for (int i = 0; i < p1; ++i) {
                p2 = ints[i].length;
                res[i] = new String[p2];
                for (int j = 0; j < p2; ++j) {
                    res[i][j] = dataModel.getEffectiveDimensions().get(ints[i][j]).getIdentity();
                }
            }
        } else {
            res = new String[0][0];
        }
        return res;
    }

}
