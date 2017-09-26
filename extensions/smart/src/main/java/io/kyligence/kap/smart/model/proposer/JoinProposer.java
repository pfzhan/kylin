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

package io.kyligence.kap.smart.model.proposer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelDesc.TableKind;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import io.kyligence.kap.smart.model.ModelContext;
import io.kyligence.kap.smart.util.JoinDescUtil;

public class JoinProposer extends AbstractModelProposer {

    public JoinProposer(ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    public void doPropose(DataModelDesc modelDesc) {

        Map<String, JoinTableDesc> joinTables = new HashMap<>();
        Map<TableRef, String> tableAliasMap = getModelContext().getAllTableRefAlias();

        for (OLAPContext ctx : getModelContext().getAllOLAPContexts()) {
            if (ctx == null || ctx.joins == null || ctx.joins.size() == 0) {
                continue;
            }

            // Save context updates and apply later
            Map<String, JoinTableDesc> joinTablesModification = new HashMap<>();
            boolean skipModification = false;

            List<TableKind> tableKindByJoins = JoinDescUtil.resolveTableType(ctx.joins);

            for (int i = 0; i < ctx.joins.size(); i++) {
                JoinDesc join = ctx.joins.get(i);
                TableKind kind = tableKindByJoins.get(i);
                String pkTblAlias = tableAliasMap.get(join.getPKSide());
                String fkTblAlias = tableAliasMap.get(join.getFKSide());

                JoinTableDesc joinTable = JoinDescUtil.convert(join, kind, pkTblAlias, fkTblAlias);

                String joinTableAlias = joinTable.getAlias();
                JoinTableDesc oldJoinTable = joinTables.get(joinTableAlias);
                if (oldJoinTable == null) {
                    oldJoinTable = joinTablesModification.get(joinTableAlias);
                }
                if (oldJoinTable == null) {
                    joinTablesModification.put(joinTableAlias, joinTable);
                    continue;
                }

                // duplication check
                if (oldJoinTable.equals(joinTable)) {
                    continue;
                }
                // conflict check
                if (!JoinDescUtil.isJoinKeysEqual(oldJoinTable.getJoin(), joinTable.getJoin())) {
                    // add and resolve alias 
                    String newAlias = getNewAlias(tableAliasMap.values(), joinTable.getAlias());
                    joinTable.setAlias(newAlias);
                    joinTablesModification.put(newAlias, JoinDescUtil.convert(join, kind, newAlias, fkTblAlias));
                    tableAliasMap.put(join.getPKSide(), newAlias);
                    continue;
                }
                if (!JoinDescUtil.isJoinTypeEqual(oldJoinTable.getJoin(), joinTable.getJoin())) {
                    // join conflict inner <-> left
                    skipModification = true;
                    break;
                }
                if (!oldJoinTable.getKind().equals(joinTable.getKind())) {
                    // LOOKUP vs FACT, use FACT
                    joinTable.setKind(TableKind.FACT);
                    joinTablesModification.put(joinTable.getAlias(), joinTable);
                }
            }
            if (!skipModification) {
                joinTables.putAll(joinTablesModification);
            }
        }

        // Add joins
        modelDesc.setJoinTables(joinTables.values().toArray(new JoinTableDesc[0]));
        // add initial scope
        modelDesc.setDimensions(new ArrayList<ModelDimensionDesc>(0));
        modelDesc.setMetrics(new String[0]);
    }

    public static String getNewAlias(Collection<String> aliasSet, String oldAlias) {
        String newAlias = oldAlias;
        int i = 1;
        while (aliasSet.contains(newAlias)) {
            newAlias = oldAlias + "_" + i;
            i++;
        }
        return newAlias;
    }

    public List<TableRef> getTableRefByAlias(Map<TableRef, String> tableAliasMap, String alias) {
        List<TableRef> result = new ArrayList<>();
        for (Entry<TableRef, String> entry : tableAliasMap.entrySet()) {
            if (entry.getKey().equals(alias)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }
}
