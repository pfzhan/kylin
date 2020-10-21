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

import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;

public interface NonEquiJoinConditionVisitor {

    default NonEquiJoinCondition visit(NonEquiJoinCondition nonEquiJoinCondition) {
        if (nonEquiJoinCondition == null) {
            return null;
        }

        if (nonEquiJoinCondition.getType() == NonEquiJoinConditionType.LITERAL) {
            return visitLiteral(nonEquiJoinCondition);
        } else if (nonEquiJoinCondition.getType() == NonEquiJoinConditionType.COLUMN) {
            return visitColumn(nonEquiJoinCondition);
        } else if (nonEquiJoinCondition.getType() == NonEquiJoinConditionType.EXPRESSION) {
            return visitExpression(nonEquiJoinCondition);
        } else {
            return visit(nonEquiJoinCondition);
        }
    }

    default NonEquiJoinCondition visitExpression(NonEquiJoinCondition nonEquiJoinCondition) {
        NonEquiJoinCondition[] ops = new NonEquiJoinCondition[nonEquiJoinCondition.getOperands().length];
        for (int i = 0; i < nonEquiJoinCondition.getOperands().length; i++) {
            ops[i] = visit(nonEquiJoinCondition.getOperands()[i]);
        }
        nonEquiJoinCondition.setOperands(ops);
        return nonEquiJoinCondition;
    }

    default NonEquiJoinCondition visitColumn(NonEquiJoinCondition nonEquiJoinCondition) {
        return nonEquiJoinCondition;
    }


    default NonEquiJoinCondition visitLiteral(NonEquiJoinCondition nonEquiJoinCondition) {
        return nonEquiJoinCondition;
    }

}
