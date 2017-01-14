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

package io.kyligence.kap.modeling.auto.util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.TblColRef;

public class CubeDescUtil {
    public static RowKeyColDesc getRowKeyColDescByName(CubeDesc cubeDesc, String rowkeyName) {
        TblColRef tblColRef = cubeDesc.getModel().findColumn(rowkeyName);
        return cubeDesc.getRowkey().getColDesc(tblColRef);
    }

    public static void addRowKeyToAggGroup(AggregationGroup aggGroup, String rowKeyName) {
        if (aggGroup == null) {
            return;
        }

        if (aggGroup.getIncludes() == null) {
            aggGroup.setIncludes(new String[0]);
        }

        if (!ArrayUtils.contains(aggGroup.getIncludes(), rowKeyName)) {
            String[] includes = new String[aggGroup.getIncludes().length + 1];
            System.arraycopy(aggGroup.getIncludes(), 0, includes, 0, aggGroup.getIncludes().length);
            includes[includes.length - 1] = rowKeyName;
            aggGroup.setIncludes(includes);
        }
    }

    public static void removeRowKeyFromAggGroup(AggregationGroup aggGroup, String rowKeyName) {
        if (aggGroup == null || aggGroup.getIncludes() == null) {
            return;
        }

        if (ArrayUtils.contains(aggGroup.getIncludes(), rowKeyName)) {
            aggGroup.setIncludes((String[]) ArrayUtils.removeElement(aggGroup.getIncludes(), rowKeyName));

            SelectRule selectRule = aggGroup.getSelectRule();
            if (selectRule != null) {
                if (ArrayUtils.contains(selectRule.mandatory_dims, rowKeyName)) {
                    ArrayUtils.removeElement(selectRule.mandatory_dims, rowKeyName);
                } else {
                    for (String[] joints : selectRule.joint_dims) {
                        if (ArrayUtils.contains(joints, rowKeyName)) {
                            ArrayUtils.removeElement(joints, rowKeyName);
                            return;
                        }
                    }
                    for (String[] hiers : selectRule.hierarchy_dims) {
                        if (ArrayUtils.contains(hiers, rowKeyName)) {
                            ArrayUtils.removeElement(hiers, rowKeyName);
                            return;
                        }
                    }
                }
            }
        }
    }
}
