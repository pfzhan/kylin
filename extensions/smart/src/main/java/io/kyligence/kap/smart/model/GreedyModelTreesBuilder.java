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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.JoinsTree.Chain;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.util.JoinDescUtil;
import io.kyligence.kap.smart.util.OLAPContextUtil;
import io.kyligence.kap.smart.util.TableAliasGenerator;

public class GreedyModelTreesBuilder {
    private final Map<String, TableDesc> tableMap;
    KylinConfig kylinConfig;

    public GreedyModelTreesBuilder(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        this.tableMap = NTableMetadataManager.getInstance(kylinConfig, project).getAllTablesMap();
    }

    public List<ModelTree> build(List<String> sqls, List<Collection<OLAPContext>> olapContexts,
            TableDesc expectTactTbl) {
        // 1. group OLAPContexts by fact_table
        Map<TableDesc, TreeBuilder> builders = Maps.newHashMap();
        for (int i = 0; i < sqls.size(); i++) {
            String sql = sqls.get(i);
            for (OLAPContext ctx : olapContexts.get(i)) {
                if (ctx.firstTableScan == null) { // no model required
                    continue;
                }

                TableDesc actualFactTbl = ctx.firstTableScan.getTableRef().getTableDesc();
                if (expectTactTbl != null && !actualFactTbl.getIdentity().equals(expectTactTbl.getIdentity())) { // root fact not match
                    continue;
                }

                TreeBuilder builder = builders.get(actualFactTbl);
                if (builder == null) {
                    builder = new TreeBuilder(actualFactTbl);
                    builders.put(actualFactTbl, builder);
                }

                builder.addOLAPContext(sql, ctx);
            }
        }

        // 2. each group generate multiple ModelTrees
        List<ModelTree> results = Lists.newLinkedList();
        for (Map.Entry<TableDesc, TreeBuilder> entry : builders.entrySet()) {
            results.addAll(entry.getValue().build());
        }

        // 3. enable current root_fact's model exists
        if (expectTactTbl != null) {
            boolean needAdd = true;
            for (ModelTree tree : results) {
                if (tree.getRootFactTable() == expectTactTbl) {
                    needAdd = false;
                }
            }

            if (needAdd) {
                results.add(new ModelTree(expectTactTbl, CollectionUtils.EMPTY_COLLECTION, MapUtils.EMPTY_MAP,
                        MapUtils.EMPTY_MAP));
            }
        }
        return results;
    }
    


    boolean matchContext(List<OLAPContext> ctxs, OLAPContext anotherCtx) {
        for (OLAPContext olapContext : ctxs) {
            if (!matchContext(olapContext, anotherCtx)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Strictly check contexts' consistency
     * 
     * @param ctx
     * @param anotherCtx
     * @return
     */
    boolean matchContext(OLAPContext ctx, OLAPContext anotherCtx) {
        List<JoinDesc> joins = Lists.newArrayList();
        if (ctx != null && ctx.joins != null) {
            joins.addAll(ctx.joins);
        }

        List<JoinDesc> anotherJoins = Lists.newArrayList();
        if (anotherCtx != null && anotherCtx.joins != null) {
            anotherJoins.addAll(anotherCtx.joins);
        }

//        if (ctx.getSQLDigest().isRawQuery != anotherCtx.getSQLDigest().isRawQuery) {
//            return false;
//        }

        JoinsTree tree = new JoinsTree(ctx.firstTableScan.getTableRef(), joins);
        JoinsTree anotherTree = new JoinsTree(anotherCtx.firstTableScan.getTableRef(), anotherJoins);
        return matchJoinTree(tree, anotherTree);
    }
    
    public static boolean matchJoinTree(JoinsTree tree, JoinsTree anotherTree) {
        List<Chain> chains = tree.unmatchedChain(anotherTree, Collections.<String, String>emptyMap());
        for (Chain chain : chains) {
            if (!chain.getJoin().isLeftJoin()) {
                return false;
            }
        }

        chains = anotherTree.unmatchedChain(tree, Collections.<String, String>emptyMap());
        for (Chain chain : chains) {
            if (!chain.getJoin().isLeftJoin()) {
                return false;
            }
        }

        return true;
    }

    private class TreeBuilder {
        TableDesc rootFact;
        TableAliasGenerator.TableAliasDict dict;

        Map<String, Collection<OLAPContext>> contexts = Maps.newLinkedHashMap();
        Map<TableRef, String> innerTableRefAlias = Maps.newHashMap();
        Map<TableRef, String> correctedTableAlias = Maps.newHashMap();

        TreeBuilder(TableDesc rootFact) {
            this.rootFact = rootFact;
            this.dict = TableAliasGenerator.generateNewDict(tableMap.keySet().toArray(new String[0]));
        }

        void addOLAPContext(String sql, OLAPContext ctx) {
            if (!this.contexts.containsKey(sql)) {
                this.contexts.put(sql, new ArrayList<OLAPContext>());
            }
            this.contexts.get(sql).add(ctx);
            ctx.sql = sql;
            this.innerTableRefAlias.putAll(getTableAliasMap(ctx, dict));
            correctTableAlias();
        }

        ModelTree buildOne(List<OLAPContext> inputCtxs) {
            SmartConfig smartConfig = SmartConfig.wrap(kylinConfig);

            Map<String, JoinTableDesc> joinTables = new HashMap<>();
            Map<TableRef, String> tableAliasMap = correctedTableAlias;
            List<OLAPContext> usedCtxs = Lists.newArrayList();
            Map<String, TableRef> aliasRefMap = Maps.newHashMap();
            for (OLAPContext ctx : inputCtxs) {
                if (ctx == null) {
                    usedCtxs.add(ctx);
                    continue;
                }
                
                if (smartConfig.enableModelInnerJoinExactlyMatch() && !matchContext(usedCtxs, ctx)) {
                    // ctx not fit current tree
                    continue;
                }
                
                if (ctx.joins == null || ctx.joins.size() == 0) {
                    usedCtxs.add(ctx);
                    continue;
                }

                // Save context updates and apply later
                Map<String, JoinTableDesc> joinTablesUpdates = new LinkedHashMap<>(joinTables);
                Map<TableRef, String> tableAliasUpdates = new LinkedHashMap<>(tableAliasMap);
                boolean skipModification = false;

                List<NDataModel.TableKind> tableKindByJoins = JoinDescUtil.resolveTableType(ctx.joins);

                for (int i = 0; i < ctx.joins.size(); i++) {
                    JoinDesc join = ctx.joins.get(i);
                    NDataModel.TableKind kind = tableKindByJoins.get(i);
                    String pkTblAlias = tableAliasUpdates.get(join.getPKSide());
                    String fkTblAlias = tableAliasUpdates.get(join.getFKSide());

                    String joinTableAlias = pkTblAlias;

                    while (!skipModification) {
                        JoinTableDesc joinTable = JoinDescUtil.convert(join, kind, joinTableAlias, fkTblAlias,
                                aliasRefMap);
                        JoinTableDesc oldJoinTable = joinTablesUpdates.get(joinTableAlias);

                        // new join table
                        if (oldJoinTable == null) {
                            joinTablesUpdates.put(joinTableAlias, joinTable);
                            tableAliasUpdates.put(join.getPKSide(), joinTableAlias);
                            break;
                        }

                        // duplicated join table
                        if (JoinDescUtil.isJoinTableEqual(oldJoinTable, joinTable)) {
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
                            kind = NDataModel.TableKind.FACT;
                            joinTablesUpdates.remove(oldJoinTable.getAlias());
                        }
                    }
                }
                if (!skipModification) {
                    joinTables.putAll(joinTablesUpdates);
                    tableAliasMap.putAll(tableAliasUpdates);
                    usedCtxs.add(ctx);
                }
            }

            inputCtxs.removeAll(usedCtxs);
            return new ModelTree(rootFact, usedCtxs, joinTables, correctedTableAlias);
        }

        List<ModelTree> build() {
            List<OLAPContext> ctxs = Lists.newArrayList();
            for (Map.Entry<String, Collection<OLAPContext>> entry : contexts.entrySet()) {
                ctxs.addAll(entry.getValue());
            }

            List<ModelTree> result = Lists.newArrayList();
            while (!ctxs.isEmpty()) {
                result.add(buildOne(ctxs));
            }
            return result;
        }

        private void correctTableAlias() {
            Map<String, TableDesc> classifiedAlias = new HashMap<>();
            for (Map.Entry<TableRef, String> entry : innerTableRefAlias.entrySet()) {
                classifiedAlias.put(entry.getValue(), entry.getKey().getTableDesc());
            }
            Map<String, String> orig2corrected = new HashMap<>();
            // correct fact table alias in 1st place
            String factTableName = rootFact.getName();
            orig2corrected.put(factTableName, factTableName);
            classifiedAlias.remove(factTableName);
            for (Map.Entry<String, TableDesc> entry : classifiedAlias.entrySet()) {
                String original = entry.getKey();
                String tableName = entry.getValue().getName();
                String corrected = tableName;
                int i = 1;
                while (orig2corrected.containsValue(corrected)) {
                    corrected = tableName + "_" + i;
                    i++;
                }
                orig2corrected.put(original, corrected);
            }
            for (Map.Entry<TableRef, String> entry : innerTableRefAlias.entrySet()) {
                String corrected = orig2corrected.get(entry.getValue());
                correctedTableAlias.put(entry.getKey(), corrected);
            }
        }

        private Map<TableRef, String> getTableAliasMap(OLAPContext ctx, TableAliasGenerator.TableAliasDict dict) {
            JoinsTree joinsTree = ctx.joinsTree;
            if (joinsTree == null) {
                joinsTree = new JoinsTree(ctx.firstTableScan.getTableRef(), ctx.joins);
            }

            Map<TableRef, String> allTableAlias = new HashMap<>();
            TableRef[] allTables = OLAPContextUtil.getAllTableRef(ctx);

            for (TableRef tableRef : allTables) {
                TableRef[] joinHierarchy = getJoinHierarchy(joinsTree, tableRef);
                String[] tableNames = new String[joinHierarchy.length];

                for (int i = 0; i < joinHierarchy.length; i++) {
                    TableRef table = joinHierarchy[i];
                    tableNames[i] = table.getTableIdentity();
                }

                String tblAlias = (joinHierarchy.length == 1 && joinHierarchy[0] == ctx.firstTableScan.getTableRef())
                        ? ctx.firstTableScan.getTableRef().getTableName()
                        : dict.getHierachyAlias(tableNames);

                allTableAlias.put(tableRef, tblAlias);
            }
            return allTableAlias;
        }

        private TableRef[] getJoinHierarchy(JoinsTree joinsTree, TableRef leaf) {
            if (leaf == null) {
                return new TableRef[0];
            }

            JoinDesc join = joinsTree.getJoinByPKSide(leaf);
            if (join == null) {
                return new TableRef[] { leaf };
            }

            return (TableRef[]) ArrayUtils.add(getJoinHierarchy(joinsTree, join.getFKSide()), leaf);
        }

        /**
         * get new alias by original table name, for table 'foo'
         *   foo -> foo_1
         *   foo_1 -> foo_2
         *
         * @param orginalName
         * @param oldAlias
         * @return
         */
        private String getNewAlias(String orginalName, String oldAlias) {
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
}
