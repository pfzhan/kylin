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

package org.apache.kylin.metadata.model.tool;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.sql.SqlKind;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;

public class NonEquiJoinConditionComparator {

    public static boolean equals(NonEquiJoinCondition cond1, NonEquiJoinCondition cond2) {
        if (cond1.getType() == cond2.getType() && cond1.getOp() == cond2.getOp() && Objects.equals(cond1.getOpName(), cond2.getOpName())) {
            if (cond1.getType() == NonEquiJoinConditionType.EXPRESSION) {
                if (cond1.getOperands().length == cond2.getOperands().length) {
                    if (cond1.getOperands().length > 0) {
                        return compareExpression(cond1, cond2);
                    } else {
                        return true; // func with on operands
                    }
                }
            } else if (cond1.getType() == NonEquiJoinConditionType.COLUMN) {
                return Objects.equals(cond1.getColRef().getColumnDesc(),
                        cond2.getColRef().getColumnDesc());
            } else {
                return Objects.equals(cond1.getDataType(), cond2.getDataType()) && Objects.equals(cond1.getValue(), cond2.getValue());
            }
        }
        return false;
    }

    public static boolean compareExpression(NonEquiJoinCondition cond1, NonEquiJoinCondition cond2) {
        if (cond1.getOp() == SqlKind.AND || cond1.getOp() == SqlKind.OR) { //.getOp()s that.getOperands() orderings does not matter
            Set<Integer> matchedCond2Inputs = new HashSet<>();
            for (int i = 0; i < cond1.getOperands().length; i++) {
                for (int j = 0; j < cond2.getOperands().length; j++) {
                    if (!matchedCond2Inputs.contains(j) && equals(cond1.getOperands()[i], cond2.getOperands()[j])) {
                        matchedCond2Inputs.add(j);
                        break;
                    }
                }
            }
            return matchedCond2Inputs.size() == cond2.getOperands().length;
        } else {
            for (int i = 0; i < cond1.getOperands().length; i++) {
                if (!equals(cond1.getOperands()[i], cond2.getOperands()[i])) {
                    return false;
                }
            }
            return true;
        }
    }
}
