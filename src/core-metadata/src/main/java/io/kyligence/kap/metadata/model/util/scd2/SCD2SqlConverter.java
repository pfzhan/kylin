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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TableRef;

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

        sb.append("select * from ").append(toJoinDescQuotedString(joinDesc))
                .append(" " + SqlKind.AND.sql + " " + genNonEquiWithSimplified(simplifiedNonEquiJoinConditions));

        return sb.toString();
    }

    private String quotedIdentifierStr(String identifier) {
        return Quoting.DOUBLE_QUOTE.string + identifier + Quoting.DOUBLE_QUOTE.string;
    }

    private String quotedTableRefStr(TableRef tableRef) {
        return quotedIdentifierStr(tableRef.getTableDesc().getDatabase()) + "."
                + quotedIdentifierStr(tableRef.getTableDesc().getName());
    }

    private String quotedColumnStr(String colStr) {
        String[] cols = StringSplitter.split(colStr, ".");
        return quotedIdentifierStr(cols[0]) + "." + quotedIdentifierStr(cols[1]);
    }

    private String genNonEquiWithSimplified(List<NonEquiJoinCondition.SimplifiedNonEquiJoinCondition> simplified) {

        return simplified.stream()
                .map(simplifiedNonEquiJoinCondition -> "("
                        + quotedColumnStr(simplifiedNonEquiJoinCondition.getForeignKey())
                        + simplifiedNonEquiJoinCondition.getOp().sql
                        + quotedColumnStr(simplifiedNonEquiJoinCondition.getPrimaryKey()) + ")")
                .collect(Collectors.joining(" " + SqlKind.AND.sql + " "));

    }

    private String toJoinDescQuotedString(JoinDesc join) {

        StringBuilder result = new StringBuilder();
        result.append(" ").append(quotedTableRefStr(join.getFKSide())).append(" AS ")
                .append(quotedIdentifierStr(join.getFKSide().getAlias())).append(" ").append(join.getType())
                .append(" JOIN ").append(quotedTableRefStr(join.getPKSide())).append(" AS ")
                .append(quotedIdentifierStr(join.getPKSide().getAlias())).append(" ON ");
        for (int i = 0; i < join.getForeignKey().length; i++) {
            String fk = quotedColumnStr(join.getForeignKey()[i]);
            String pk = quotedColumnStr(join.getPrimaryKey()[i]);
            if (i > 0) {
                result.append(" " + SqlKind.AND.sql + " ");
            }
            result.append(fk).append("=").append(pk);
        }
        return result.toString();
    }
}
