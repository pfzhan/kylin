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
import java.util.Objects;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.metadata.model.NDataModel.TableKind;

public class JoinDescUtil {

    private JoinDescUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static JoinTableDesc convert(JoinDesc join, TableKind kind, String pkTblAlias, String fkTblAlias,
            Map<String, TableRef> aliasTableRefMap) {
        if (join == null) {
            return null;
        }

        TableRef table = join.getPKSide();
        JoinTableDesc joinTableDesc = new JoinTableDesc();
        joinTableDesc.setKind(kind);
        joinTableDesc.setTable(table.getTableIdentity());
        joinTableDesc.setAlias(pkTblAlias);

        JoinDesc.JoinDescBuilder joinDescBuilder = new JoinDesc.JoinDescBuilder();

        joinDescBuilder.setType(join.getType().toLowerCase());
        String[] pkCols = new String[join.getPrimaryKey().length];
        TblColRef[] pkColRefs = new TblColRef[pkCols.length];

        TableRef pkTblRef = aliasTableRefMap.computeIfAbsent(pkTblAlias, alias -> TblColRef.tableForUnknownModel(alias,
                join.getPKSide().getTableDesc()));
        for (int i = 0; i < pkCols.length; i++) {
            TblColRef colRef = join.getPrimaryKeyColumns()[i];
            pkCols[i] = pkTblAlias + "." + colRef.getName();
            pkColRefs[i] = TblColRef.columnForUnknownModel(pkTblRef, colRef.getColumnDesc());
        }
        joinDescBuilder.addPrimaryKeys(pkCols, pkColRefs);
        joinDescBuilder.setPrimaryTableRef(pkTblRef);

        String[] fkCols = new String[join.getForeignKey().length];
        TblColRef[] fkColRefs = new TblColRef[fkCols.length];

        TableRef fkTblRef = aliasTableRefMap.computeIfAbsent(fkTblAlias, alias -> TblColRef.tableForUnknownModel(alias,
                join.getFKSide().getTableDesc()));
        for (int i = 0; i < fkCols.length; i++) {
            TblColRef colRef = join.getForeignKeyColumns()[i];
            fkCols[i] = fkTblAlias + "." + colRef.getName();
            fkColRefs[i] = TblColRef.columnForUnknownModel(fkTblRef, colRef.getColumnDesc());
        }
        joinDescBuilder.addForeignKeys(fkCols, fkColRefs);
        joinDescBuilder.setForeignTableRef(fkTblRef);

        if (join.getNonEquiJoinCondition() != null) {
            NonEquiJoinCondition nonEquiJoinCondition = convertNonEquiJoinCondition(join.getNonEquiJoinCondition(), pkTblRef, fkTblRef);
            String expr = join.getNonEquiJoinCondition().getExpr();
            expr = expr.replaceAll(join.getPKSide().getAlias(), pkTblAlias);
            expr = expr.replaceAll(join.getFKSide().getAlias(), fkTblAlias);
            nonEquiJoinCondition.setExpr(expr);
            joinDescBuilder.setNonEquiJoinCondition(nonEquiJoinCondition);
        }
        joinTableDesc.setJoin(joinDescBuilder.build());

        return joinTableDesc;
    }

    private static NonEquiJoinCondition convertNonEquiJoinCondition(NonEquiJoinCondition cond, TableRef pkTblRef, TableRef fkTblRef) {
        if (cond.getType() == NonEquiJoinConditionType.EXPRESSION) {
            return new NonEquiJoinCondition(
                    cond.getOpName(),
                    cond.getOp(),
                    Arrays.stream(cond.getOperands()).map(condInput -> convertNonEquiJoinCondition(condInput, pkTblRef, fkTblRef)).toArray(NonEquiJoinCondition[]::new),
                    cond.getDataType()
            );
        } else if (cond.getType() == NonEquiJoinConditionType.LITERAL) {
            return cond;
        } else {
            return new NonEquiJoinCondition(convertColumn(cond.getColRef(), pkTblRef, fkTblRef), cond.getDataType());
        }
    }

    private static TblColRef convertColumn(TblColRef colRef, TableRef pkTblRef, TableRef fkTblRef) {
        if (colRef.getTableRef().getTableIdentity().equals(pkTblRef.getTableIdentity())) {
            return TblColRef.columnForUnknownModel(pkTblRef, colRef.getColumnDesc());
        } else {
            return TblColRef.columnForUnknownModel(fkTblRef, colRef.getColumnDesc());
        }
    }

    public static List<Pair<JoinDesc, TableKind>> resolveTableType(List<JoinDesc> joins) {
        List<Pair<JoinDesc, TableKind>> tableKindByJoins = new ArrayList<>();
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
            //            if (fkTables.containsKey(tableAlias)) {
            tableKindByJoins.add(new Pair<JoinDesc, TableKind>(joinDesc, TableKind.FACT));
            //            } else {
            //            tableKindByJoins.add(new Pair<JoinDesc, TableKind>(joinDesc, TableKind.LOOKUP));
            //            }
        }
        return tableKindByJoins;
    }

    public static boolean isJoinTypeEqual(JoinDesc a, JoinDesc b) {
        return (a.isInnerJoin() && b.isInnerJoin()) || (a.isLeftJoin() && b.isLeftJoin());
    }

    public static boolean isJoinTableEqual(JoinTableDesc a, JoinTableDesc b) {
        if (a == b)
            return true;

        if (!a.getTable().equalsIgnoreCase(b.getTable()))
            return false;
        if (a.getKind() != b.getKind())
            return false;
        if (!a.getAlias().equalsIgnoreCase(b.getAlias()))
            return false;

        JoinDesc ja = a.getJoin();
        JoinDesc jb = b.getJoin();
        if (!ja.getType().equalsIgnoreCase(jb.getType()))
            return false;
        if (!Arrays.equals(ja.getForeignKey(), jb.getForeignKey()))
            return false;
        if (!Arrays.equals(ja.getPrimaryKey(), jb.getPrimaryKey()))
            return false;
        if (!Objects.equals(ja.getNonEquiJoinCondition(), jb.getNonEquiJoinCondition())) {
            return false;
        }
        return true;
    }

    public static boolean isJoinKeysEqual(JoinDesc a, JoinDesc b) {
        if (!Arrays.equals(a.getForeignKey(), b.getForeignKey()))
            return false;
        if (!Arrays.equals(a.getPrimaryKey(), b.getPrimaryKey()))
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
