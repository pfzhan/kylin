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

package io.kyligence.kap.smart.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.DataModelDesc.TableKind;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;

public class JoinDescUtil {

    public static JoinTableDesc convert(JoinDesc join, TableKind kind, String pkTblAlias, String fkTblAlias) {
        if (join == null) {
            return null;
        }

        TableRef table = join.getPKSide();
        JoinTableDesc joinTableDesc = new JoinTableDesc();
        joinTableDesc.setKind(kind);
        joinTableDesc.setTable(table.getTableIdentity());
        joinTableDesc.setAlias(pkTblAlias);

        JoinDesc joinDesc = new JoinDesc();

        joinDesc.setType(join.getType().toLowerCase());
        String[] pkCols = new String[join.getPrimaryKey().length];
        for (int i = 0; i < pkCols.length; i++) {
            pkCols[i] = pkTblAlias + "." + join.getPrimaryKeyColumns()[i].getName();
        }
        joinDesc.setPrimaryKey(pkCols);

        String[] fkCols = new String[join.getForeignKey().length];
        for (int i = 0; i < fkCols.length; i++) {
            fkCols[i] = fkTblAlias + "." + join.getForeignKeyColumns()[i].getName();
        }
        joinDesc.setForeignKey(fkCols);
        joinTableDesc.setJoin(joinDesc);

        return joinTableDesc;
    }

    public static List<TableKind> resolveTableType(List<JoinDesc> joins) {
        List<TableKind> tableKindByJoins = new ArrayList<>();
        Map<String, JoinDesc> fkTables = new HashMap<>();
        for (JoinDesc joinDesc : joins) {
            TableRef table = joinDesc.getFKSide();
            String tableAlias = table.getAlias();
            if (fkTables.containsKey(tableAlias)) {
                // error
            }
            fkTables.put(tableAlias, joinDesc);
        }
        for (JoinDesc joinDesc : joins) {
            TableRef table = joinDesc.getPKSide();
            String tableAlias = table.getAlias();
            if (fkTables.containsKey(tableAlias)) {
                tableKindByJoins.add(TableKind.FACT);
            } else {
                tableKindByJoins.add(TableKind.LOOKUP);
            }
        }
        return tableKindByJoins;
    }

    public static boolean isJoinTypeEqual(JoinDesc a, JoinDesc b) {
        return (a.isInnerJoin() && b.isInnerJoin()) || (a.isLeftJoin() && b.isLeftJoin());
    }

    public static boolean isJoinKeysEqual(JoinDesc a, JoinDesc b) {
        if (!Arrays.equals(a.getForeignKey(), b.getForeignKey()))
            return false;
        if (!Arrays.equals(a.getPrimaryKey(), b.getPrimaryKey()))
            return false;
        if (!Arrays.equals(a.getForeignKeyColumns(), b.getForeignKeyColumns()))
            return false;
        if (!Arrays.equals(a.getPrimaryKeyColumns(), b.getPrimaryKeyColumns()))
            return false;
        return true;
    }

    public static String toString(JoinTableDesc join) {
        StringBuilder result = new StringBuilder();
        result.append(join.getJoin().getType()).append(" JOIN ").append(join.getTable()).append(" AS ")
                .append(join.getAlias()).append(" ON ");
        for (int i = 0; i < join.getJoin().getForeignKey().length; i++) {
            String fk = join.getJoin().getForeignKey()[i];
            String pk = join.getJoin().getPrimaryKey()[i];
            if (i > 0) {
                result.append(" AND ");
            }
            result.append(fk).append("=").append(pk);
        }
        return result.toString();
    }
}
