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
package io.kyligence.kap.metadata.model.util.scd2;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;

import io.kyligence.kap.metadata.model.util.JoinDescUtil;

/**
 * convert simplified cond to sql
 */
public class SCD2SqlConverter {

    public static final SCD2SqlConverter INSTANCE = new SCD2SqlConverter();

    /**
     * SimplifiedNonEquiJoinCondition must not be empty
     *
     * you should check scd2 expression before call this method
     * @param joinDesc
     * @return
     */
    public String genSCD2SqlStr(JoinDesc joinDesc,
            List<NonEquiJoinCondition.SimplifiedNonEquiJoinCondition> simplifiedNonEquiJoinConditions) {
        StringBuilder sb = new StringBuilder();

        sb.append("select * from ").append(JoinDescUtil.toString(joinDesc))
                .append(" AND " + genNonEquiWithSimplified(simplifiedNonEquiJoinConditions));

        return sb.toString();
    }

    private String genNonEquiWithSimplified(List<NonEquiJoinCondition.SimplifiedNonEquiJoinCondition> simplified) {

        String simpleExpr = simplified.stream()
                .map(simplifiedNonEquiJoinCondition -> "(" + simplifiedNonEquiJoinCondition.getForeignKey()
                        + simplifiedNonEquiJoinCondition.getOp().sql + simplifiedNonEquiJoinCondition.getPrimaryKey()
                        + ")")
                .collect(Collectors.joining(" AND "));

        return simpleExpr;

    }
}
