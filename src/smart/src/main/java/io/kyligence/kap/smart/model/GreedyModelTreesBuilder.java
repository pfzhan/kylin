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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.TableKind;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
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

    @SuppressWarnings("unchecked")
    public List<ModelTree> build(List<String> sqls, List<Collection<OLAPContext>> olapContexts,
            TableDesc expectTactTbl) {
        // 1. group OLAPContexts by fact_table
        Map<TableDesc, TreeBuilder> builders = Maps.newHashMap();
        for (int i = 0; i < sqls.size(); i++) {
            String sql = sqls.get(i);
            Collection<OLAPContext> sqlContexts = olapContexts.get(i);
            sqlContexts.stream() //
                    .filter(ctx -> ctx.firstTableScan != null) //
                    .forEach(ctx -> {
                        TableDesc actualFactTbl = ctx.firstTableScan.getTableRef().getTableDesc();
                        if (expectTactTbl != null && !actualFactTbl.getIdentity().equals(expectTactTbl.getIdentity())) {
                            return; // root fact not match
                        }

                        TreeBuilder builder = builders.computeIfAbsent(actualFactTbl,
                                tbl -> new TreeBuilder(tbl, tableMap));
                        builder.addOLAPContext(sql, ctx);
                    });
        }

        // 2. each group generate multiple ModelTrees
        List<ModelTree> results = builders.values() //
                .stream() //
                .map(TreeBuilder::build) //
                .flatMap(List::stream) //
                .collect(Collectors.toList());

        // 3. enable current root_fact's model exists
        if (expectTactTbl != null && results.stream().noneMatch(tree -> tree.getRootFactTable() == expectTactTbl)) {
            results.add(new ModelTree(expectTactTbl, CollectionUtils.EMPTY_COLLECTION, MapUtils.EMPTY_MAP,
                    MapUtils.EMPTY_MAP));
        }
        return results;
    }

    public static boolean matchContext(List<OLAPContext> ctxs, OLAPContext anotherCtx) {
        return ctxs.stream().allMatch(thisCtx -> matchContext(thisCtx, anotherCtx));
    }

    public static boolean matchContext(OLAPContext ctxA, OLAPContext ctxB) {
        if (ctxA == ctxB) {
            return true;
        }
        if (ctxA == null || ctxB == null) {
            return false;
        }
        JoinsGraph graphA = new JoinsGraph(ctxA.firstTableScan.getTableRef(), Lists.newArrayList(ctxA.joins));
        JoinsGraph graphB = new JoinsGraph(ctxB.firstTableScan.getTableRef(), Lists.newArrayList(ctxB.joins));
        return graphA.match(graphB, Maps.newHashMap()) //
                || graphB.match(graphA, Maps.newHashMap())
                || (graphA.unmatched(graphB).stream().allMatch(JoinsGraph.Edge::isLeftJoin)
                        && graphB.unmatched(graphA).stream().allMatch(JoinsGraph.Edge::isLeftJoin));
    }

    public static class TreeBuilder {
        private TableDesc rootFact;
        private TableAliasGenerator.TableAliasDict dict;

        private Map<String, Collection<OLAPContext>> contexts = Maps.newLinkedHashMap();
        private Map<TableRef, String> innerTableRefAlias = Maps.newHashMap();
        private Map<TableRef, String> correctedTableAlias = Maps.newHashMap();

        private TreeBuilder(TableDesc rootFact, Map<String, TableDesc> tableMap) {
            this.rootFact = rootFact;
            this.dict = TableAliasGenerator.generateNewDict(tableMap.keySet().toArray(new String[0]));
        }

        private void addOLAPContext(String sql, OLAPContext ctx) {
            if (!this.contexts.containsKey(sql)) {
                this.contexts.put(sql, new ArrayList<OLAPContext>());
            }
            this.contexts.get(sql).add(ctx);
            ctx.sql = sql;
            this.innerTableRefAlias.putAll(getTableAliasMap(ctx, dict));
            correctTableAlias();
        }

        private List<ModelTree> build() {
            List<OLAPContext> ctxs = contexts.values().stream().flatMap(Collection::stream)
                    .collect(Collectors.toList());

            List<ModelTree> result = Lists.newArrayList();
            while (!ctxs.isEmpty()) {
                result.add(buildOne(ctxs));
            }
            return result;
        }

        private ModelTree buildOne(List<OLAPContext> inputCtxs) {

            Map<String, JoinTableDesc> joinTables = new LinkedHashMap<>();
            Map<TableRef, String> tableAliasMap = correctedTableAlias;
            List<OLAPContext> usedCtxs = Lists.newArrayList();
            Map<String, TableRef> aliasRefMap = Maps.newHashMap();
            inputCtxs.removeIf(Objects::isNull);
            inputCtxs.stream()//
                    .filter(ctx -> matchContext(usedCtxs, ctx))//
                    .filter(ctx -> { // Digest single table contexts(no joins)
                        if (ctx.joins.isEmpty()) {
                            usedCtxs.add(ctx);
                            return false;
                        }
                        return true;
                    })//
                    .forEach(ctx -> {
                        // Merge matching contexts' joins
                        mergeContext(ctx, joinTables, tableAliasMap, aliasRefMap);
                        usedCtxs.add(ctx);
                    });

            inputCtxs.removeAll(usedCtxs);
            return new ModelTree(rootFact, usedCtxs, joinTables, correctedTableAlias);
        }

        public static void mergeContext(OLAPContext ctx, Map<String, JoinTableDesc> alias2JoinTables,
                Map<TableRef, String> tableRef2Alias, Map<String, TableRef> aliasRefMap) {

            // Collect context updates and apply later
            Map<String, JoinTableDesc> alias2JoinTablesUpdates = new LinkedHashMap<>(alias2JoinTables);
            Map<TableRef, String> tableRef2AliasUpdates = new LinkedHashMap<>(tableRef2Alias);

            List<Pair<JoinDesc, TableKind>> tableKindByJoins = JoinDescUtil.resolveTableType(ctx.joins);
            for (Pair<JoinDesc, TableKind> pair : tableKindByJoins) {
                JoinDesc join = pair.getFirst();
                TableKind kind = pair.getSecond();
                String pkTblAlias = tableRef2AliasUpdates.get(join.getPKSide());
                String fkTblAlias = tableRef2AliasUpdates.get(join.getFKSide());

                String joinTableAlias = pkTblAlias;
                boolean isValidJoin = false;
                int loops = 0;
                while (!isValidJoin) {
                    JoinTableDesc newJoinTable = JoinDescUtil.convert(join, kind, joinTableAlias, fkTblAlias,
                            aliasRefMap);
                    JoinTableDesc oldJoinTable = alias2JoinTablesUpdates.computeIfAbsent(joinTableAlias,
                            alias -> newJoinTable);

                    if (JoinDescUtil.isJoinTableEqual(oldJoinTable, newJoinTable)) {
                        isValidJoin = true;
                    } else if (JoinDescUtil.isJoinKeysEqual(oldJoinTable.getJoin(), newJoinTable.getJoin())
                            && JoinDescUtil.isJoinTypeEqual(oldJoinTable.getJoin(), newJoinTable.getJoin())
                            && !oldJoinTable.getKind().equals(newJoinTable.getKind())) {
                        // same join info but table kind differ: LOOKUP vs FACT, use FACT
                        newJoinTable.setKind(NDataModel.TableKind.FACT);
                        alias2JoinTablesUpdates.put(joinTableAlias, newJoinTable);
                    } else {
                        // twin join table with different join info
                        // resolve and assign new alias
                        joinTableAlias = getNewAlias(join.getPKSide().getTableName(), newJoinTable.getAlias());
                    }
                    if (loops++ > 100) {
                        // in case of infinite loop
                        break;
                    }
                }
                Preconditions.checkState(isValidJoin, "Failed to merge table join: %s.", join);
                tableRef2AliasUpdates.put(join.getPKSide(), joinTableAlias);
            }
            alias2JoinTables.putAll(alias2JoinTablesUpdates);
            tableRef2Alias.putAll(tableRef2AliasUpdates);
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
            JoinsGraph joinsGraph = ctx.getJoinsGraph();
            if (joinsGraph == null) {
                joinsGraph = new JoinsGraph(ctx.firstTableScan.getTableRef(), ctx.joins);
            }

            Map<TableRef, String> allTableAlias = new HashMap<>();
            TableRef[] allTables = OLAPContextUtil.getAllTableRef(ctx);

            for (TableRef tableRef : allTables) {
                TableRef[] joinHierarchy = getJoinHierarchy(joinsGraph, tableRef);
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

        private TableRef[] getJoinHierarchy(JoinsGraph joinsTree, TableRef leaf) {
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
}
