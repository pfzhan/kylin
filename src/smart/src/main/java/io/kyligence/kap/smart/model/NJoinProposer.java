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

package io.kyligence.kap.smart.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.TableKind;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.util.JoinDescUtil;

public class NJoinProposer extends NAbstractModelProposer {

    public NJoinProposer(NSmartContext.NModelContext modelContext) {
        super(modelContext);
    }

    @Override
    public void doPropose(NDataModel modelDesc) {
        ModelTree modelTree = modelContext.getModelTree();
        Map<String, JoinTableDesc> joinTables = new HashMap<>();
        Map<TableRef, String> tableAliasMap = modelTree.getTableRefAliasMap();
        Map<String, TableRef> aliasRefMap = Maps.newHashMap();

        for (JoinTableDesc joinTableDesc : modelDesc.getJoinTables()) {
            joinTables.put(joinTableDesc.getAlias(), joinTableDesc);
            tableAliasMap.put(modelDesc.findTable(joinTableDesc.getAlias()), joinTableDesc.getAlias());
        }

        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            if (ctx == null || ctx.joins == null || ctx.joins.size() == 0) {
                continue;
            }

            // Save context updates and apply later
            Map<String, JoinTableDesc> joinTablesUpdates = new HashMap<>(joinTables);
            Map<TableRef, String> tableAliasUpdates = new HashMap<>(tableAliasMap);
            boolean skipModification = false;

            List<TableKind> tableKindByJoins = JoinDescUtil.resolveTableType(ctx.joins);

            for (int i = 0; i < ctx.joins.size(); i++) {
                JoinDesc join = ctx.joins.get(i);
                TableKind kind = tableKindByJoins.get(i);
                String pkTblAlias = tableAliasUpdates.get(join.getPKSide());
                String fkTblAlias = tableAliasUpdates.get(join.getFKSide());

                String joinTableAlias = pkTblAlias;

                while (!skipModification) {
                    JoinTableDesc joinTable = JoinDescUtil.convert(join, kind, joinTableAlias, fkTblAlias, aliasRefMap);
                    JoinTableDesc oldJoinTable = joinTablesUpdates.get(joinTableAlias);

                    // new join table
                    if (oldJoinTable == null) {
                        joinTablesUpdates.put(joinTableAlias, joinTable);
                        tableAliasUpdates.put(join.getPKSide(), joinTableAlias);
                        break;
                    }

                    // duplicated join table
                    if (oldJoinTable.equals(joinTable)) {
                        tableAliasUpdates.put(join.getPKSide(), joinTableAlias);
                        break;
                    }

                    // twin join table with different join keys
                    if (!JoinDescUtil.isJoinKeysEqual(oldJoinTable.getJoin(), joinTable.getJoin())) {
                        // add and resolve alias
                        joinTableAlias = getNewAlias(join.getPKSide().getTableName(), joinTable.getAlias());
                        continue;
                    }

                    // same join keys but join type conflict: inner <-> left
                    if (!JoinDescUtil.isJoinTypeEqual(oldJoinTable.getJoin(), joinTable.getJoin())) {
                        skipModification = true;
                        break;
                    }

                    // LOOKUP vs FACT, use FACT
                    if (!oldJoinTable.getKind().equals(joinTable.getKind())) {
                        kind = TableKind.FACT;
                        joinTablesUpdates.remove(oldJoinTable.getAlias());
                        continue;
                    }

                    break;
                }
            }
            if (!skipModification) {
                joinTables.putAll(joinTablesUpdates);
                tableAliasMap.putAll(tableAliasUpdates);
            }
        }

        // Add joins
        modelDesc.setJoinTables(new ArrayList<>(joinTables.values()));
    }

    /**
     * get new alias by original table name, for table 'foo' foo -> foo_1 foo_1 ->
     * foo_2
     * 
     * @param orginalName
     * @param oldAlias
     * @return
     */
    private static String getNewAlias(String orginalName, String oldAlias) {
        if (oldAlias.equals(orginalName)) {
            return orginalName + "_1";
        } else if (!oldAlias.startsWith(orginalName + "_")) {
            return orginalName;
        }

        String number = oldAlias.substring(orginalName.length() + 1);
        try {
            Integer i = Integer.valueOf(number);
            return orginalName + "_" + (i + 1);
        } catch (Exception e) {
            return orginalName + "_1";
        }
    }

}
