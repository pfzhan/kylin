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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.TableKind;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.JoinDescUtil;
import io.kyligence.kap.query.util.QueryModelPriorities;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.util.TableAliasGenerator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GreedyModelTreesBuilder {

    private final Map<String, TableDesc> tableMap;
    KylinConfig kylinConfig;
    AbstractContext proposeContext;

    public GreedyModelTreesBuilder(KylinConfig kylinConfig, String project, AbstractContext proposeContext) {
        this.kylinConfig = kylinConfig;
        this.tableMap = NTableMetadataManager.getInstance(kylinConfig, project).getAllTablesMap();
        this.proposeContext = proposeContext;
    }

    public List<ModelTree> build(Map<String, Collection<OLAPContext>> olapContexts, TableDesc expectedFactTable) {
        // 1. group OLAPContexts by fact_table & modelPriorities
        log.info("Split OLAPContexts by fact table.");
        Map<TableDesc, Map<String, TreeBuilder>> buildersMap = Maps.newHashMap();
        olapContexts.forEach((sql, sqlContexts) -> {
            String[] modelPriorities = QueryModelPriorities.getModelPrioritiesFromComment(sql);
            sqlContexts.stream() //
                    .filter(ctx -> ctx.firstTableScan != null) //
                    .forEach(ctx -> {
                        TableDesc actualFactTbl = ctx.firstTableScan.getTableRef().getTableDesc();
                        if (expectedFactTable != null
                                && !actualFactTbl.getIdentity().equals(expectedFactTable.getIdentity())) {
                            return; // root fact not match
                        }
                        Map<String, TreeBuilder> builders = buildersMap.computeIfAbsent(actualFactTbl,
                                key -> Maps.newHashMap());
                        TreeBuilder builder = builders.computeIfAbsent(StringUtils.join(modelPriorities),
                                k -> new TreeBuilder(actualFactTbl, tableMap, proposeContext));
                        builder.addOLAPContext(sql, ctx);
                    });
        });

        // 2. each group generate multiple ModelTrees
        List<ModelTree> results = buildersMap.values() //
                .stream() //
                .flatMap(builders -> builders.values().stream())//
                .map(TreeBuilder::build) //
                .flatMap(List::stream) //
                .collect(Collectors.toList());
        log.info("Grouped OLAPContexts generated {} modelTrees.", results.size());

        // 3. enable current root_fact's model exists
        if (expectedFactTable != null
                && results.stream().noneMatch(tree -> tree.getRootFactTable() == expectedFactTable)) {
            log.debug("There is no modelTree relies on fact table({}), add a new one.",
                    expectedFactTable.getIdentity());
            results.add(new ModelTree(expectedFactTable, ImmutableList.of(), ImmutableMap.of(), ImmutableMap.of()));
        }
        return results;
    }

    public ModelTree build(Collection<OLAPContext> olapContexts, TableDesc actualFactTbl) {
        TreeBuilder treeBuilder = new TreeBuilder(actualFactTbl, tableMap, proposeContext);
        olapContexts.forEach(olapContext -> treeBuilder.addOLAPContext(olapContext.sql, olapContext));
        return treeBuilder.buildOne(olapContexts, false);
    }

    public static class TreeBuilder {
        private final TableDesc rootFact;
        private final TableAliasGenerator.TableAliasDict dict;
        private final AbstractContext proposeContext;

        private final Map<String, Collection<OLAPContext>> contexts = Maps.newLinkedHashMap();

        public TreeBuilder(TableDesc rootFact, Map<String, TableDesc> tableMap, AbstractContext proposeContext) {
            this.rootFact = rootFact;
            this.dict = TableAliasGenerator.generateNewDict(tableMap.keySet().toArray(new String[0]));
            this.proposeContext = proposeContext;
        }

        /**
         * based on the path to root node in JoinGraph to produce table alias, so that it can be unique in different ctx
         * but same position, even if the alias is not equaled in query.
         *
         * @param ctx OLAPContext
         * @param dict dict
         * @return map from TableRef to alias
         */
        static Map<TableRef, String> getUniqueTblAliasBasedOnPosInGraph(OLAPContext ctx,
                TableAliasGenerator.TableAliasDict dict) {
            JoinsGraph joinsGraph = ctx.getJoinsGraph();
            if (joinsGraph == null) {
                joinsGraph = new JoinsGraph(ctx.firstTableScan.getTableRef(), ctx.joins);
            }
            return TreeBuilder.getUniqueTblAliasBasedOnPosInGraph(joinsGraph, dict);
        }

        static Map<TableRef, String> getUniqueTblAliasBasedOnPosInGraph(JoinsGraph joinsGraph,
                TableAliasGenerator.TableAliasDict dict) {

            Map<TableRef, String> allTableAlias = new HashMap<>();
            for (TableRef tableRef : joinsGraph.getAllTblRefNodes()) {
                JoinDesc[] joinHierarchy = getJoinDescHierarchy(joinsGraph, tableRef);
                String tblAlias = (joinHierarchy.length == 0) ? joinsGraph.getCenter().getTableName()
                        : dict.getHierachyAliasFromJoins(joinHierarchy);
                allTableAlias.put(tableRef, tblAlias);
            }
            return allTableAlias;
        }

        static Map<TableRef, String> correctTblAliasAndKeepOriginAlias(Map<TableRef, String> tblRef2TreePathName,
                TableDesc rootFact, Map<TableRef, String> originTblAlias) {
            Map<String, TableDesc> classifiedAlias = new HashMap<>();
            for (Map.Entry<TableRef, String> entry : tblRef2TreePathName.entrySet()) {
                classifiedAlias.put(entry.getValue(), entry.getKey().getTableDesc());
            }

            Map<String, String> orig2corrected = new HashMap<>();
            // correct fact table alias in 1st place
            String factTableName = rootFact.getName();
            orig2corrected.put(factTableName, factTableName);
            classifiedAlias.remove(factTableName);
            for (Map.Entry<TableRef, String> entry : originTblAlias.entrySet()) {
                orig2corrected.putIfAbsent(tblRef2TreePathName.get(entry.getKey()), entry.getValue());
                classifiedAlias.remove(tblRef2TreePathName.get(entry.getKey()));
            }

            for (Map.Entry<String, TableDesc> entry : classifiedAlias.entrySet()) {
                String tableName = entry.getValue().getName();
                String corrected = tableName;
                int i = 1;
                while (orig2corrected.containsValue(corrected)) {
                    corrected = tableName + "_" + i;
                    i++;
                }
                orig2corrected.put(entry.getKey(), corrected);
            }

            Map<TableRef, String> correctedTableAlias = Maps.newHashMap();
            for (Map.Entry<TableRef, String> entry : tblRef2TreePathName.entrySet()) {
                String corrected = orig2corrected.get(entry.getValue());
                correctedTableAlias.put(entry.getKey(), corrected);
            }

            return correctedTableAlias;
        }

        private static Map<TableRef, String> correctTableAlias(Map<TableRef, String> innerTableRefAlias,
                TableDesc rootFact) {
            return correctTblAliasAndKeepOriginAlias(innerTableRefAlias, rootFact, Maps.newHashMap());
        }

        private void addOLAPContext(String sql, OLAPContext ctx) {
            if (!this.contexts.containsKey(sql)) {
                this.contexts.put(sql, new ArrayList<>());
            }
            this.contexts.get(sql).add(ctx);
            ctx.sql = sql;
        }

        private List<ModelTree> build() {
            List<OLAPContext> ctxs = contexts.values().stream().flatMap(Collection::stream)
                    .collect(Collectors.toList());

            List<ModelTree> result = Lists.newArrayList();
            while (!ctxs.isEmpty()) {
                result.add(buildOne(ctxs, false));
            }
            return result;
        }

        private ModelTree buildOne(Collection<OLAPContext> inputCtxs, boolean forceMerge) {
            Map<TableRef, String> innerTableRefAlias = Maps.newHashMap();
            Map<TableRef, String> correctedTableAlias = Maps.newHashMap();
            List<OLAPContext> usedCtxs = Lists.newArrayList();
            List<OLAPContext> ctxsNeedMerge = Lists.newArrayList();
            inputCtxs.removeIf(Objects::isNull);
            inputCtxs.stream().filter(ctx -> {
                if (forceMerge) {
                    return true;
                }
                return matchContext(usedCtxs, ctx);
            }).filter(ctx -> {
                if (ctx.joins.isEmpty()) {// Digest single table contexts(no joins)
                    innerTableRefAlias.putAll(getUniqueTblAliasBasedOnPosInGraph(ctx, dict));
                    correctedTableAlias.putAll(correctTableAlias(innerTableRefAlias, rootFact));
                    usedCtxs.add(ctx);
                    return false;
                }
                return true;
            }).forEach(context -> {
                innerTableRefAlias.putAll(getUniqueTblAliasBasedOnPosInGraph(context, dict));
                correctedTableAlias.putAll(correctTableAlias(innerTableRefAlias, rootFact));
                usedCtxs.add(context);
                ctxsNeedMerge.add(context);
            });

            Map<String, TableRef> aliasRefMap = Maps.newHashMap();
            Map<String, JoinTableDesc> joinTables = new LinkedHashMap<>();

            // Merge matching contexts' joins
            for (OLAPContext ctx : ctxsNeedMerge) {
                Map<String, AccelerateInfo> accelerateInfoMap = proposeContext.getAccelerateInfoMap();
                AccelerateInfo accelerateInfo = accelerateInfoMap.get(ctx.sql);
                if (accelerateInfo.isNotSucceed()) {
                    inputCtxs.remove(ctx);
                    usedCtxs.remove(ctx);
                    continue;
                }

                try {
                    mergeContext(ctx, joinTables, correctedTableAlias, aliasRefMap);
                } catch (Exception e) {
                    log.debug("the sql \n{}\n cannot be accelerated for meeting error", ctx.sql, e);
                    inputCtxs.remove(ctx);
                    usedCtxs.remove(ctx);
                    accelerateInfo.setFailedCause(e);
                }
            }

            inputCtxs.removeAll(usedCtxs);
            return new ModelTree(rootFact, usedCtxs, joinTables, correctedTableAlias);
        }

        public boolean matchContext(List<OLAPContext> ctxs, OLAPContext anotherCtx) {
            return ctxs.stream().allMatch(thisCtx -> matchContext(thisCtx, anotherCtx));
        }

        public boolean matchContext(OLAPContext ctxA, OLAPContext ctxB) {
            if (ctxA == ctxB) {
                return true;
            }
            if (ctxA == null || ctxB == null) {
                return false;
            }
            JoinsGraph graphA = new JoinsGraph(ctxA.firstTableScan.getTableRef(), Lists.newArrayList(ctxA.joins));
            JoinsGraph graphB = new JoinsGraph(ctxB.firstTableScan.getTableRef(), Lists.newArrayList(ctxB.joins));

            return graphA.match(graphB, Maps.newHashMap(), proposeContext.isPartialMatch(),
                    proposeContext.isPartialMatchNonEqui()) //
                    || graphB.match(graphA, Maps.newHashMap(), proposeContext.isPartialMatch(),
                            proposeContext.isPartialMatchNonEqui())
                    || (graphA.unmatched(graphB).stream().allMatch(e -> e.isLeftJoin() && !e.isNonEquiJoin())
                            && graphB.unmatched(graphA).stream().allMatch(e -> e.isLeftJoin() && !e.isNonEquiJoin()));
        }

        /**
         * @param ctx OLAPContext
         * @param alias2JoinTables unique alias name, usually depend on io.kyligence.kap.smart.util.TableAliasGenerator
         * @param tableRef2Alias map of TableRef to alias
         * @param aliasRefMap map of alias to TableRef
         */
        static void mergeContext(OLAPContext ctx, Map<String, JoinTableDesc> alias2JoinTables,
                Map<TableRef, String> tableRef2Alias, Map<String, TableRef> aliasRefMap) {
            mergeJoins(ctx.joins, alias2JoinTables, tableRef2Alias, aliasRefMap);
        }

        public AbstractContext.ModelContext mergeModelContext(AbstractContext proposeContext,
                AbstractContext.ModelContext modelContext, AbstractContext.ModelContext another) {
            List<OLAPContext> olapCtxs = Lists.newArrayList(modelContext.getModelTree().getOlapContexts());
            olapCtxs.addAll(another.getModelTree().getOlapContexts());
            return new AbstractContext.ModelContext(proposeContext, buildOne(olapCtxs, true));
        }

        private static void mergeJoins(List<JoinDesc> joins, Map<String, JoinTableDesc> alias2JoinTables,
                Map<TableRef, String> tableRef2Alias, Map<String, TableRef> aliasRefMap) {
            // Collect context updates and apply later
            Map<String, JoinTableDesc> alias2JoinTablesUpdates = new LinkedHashMap<>(alias2JoinTables);
            Map<TableRef, String> tableRef2AliasUpdates = new LinkedHashMap<>(tableRef2Alias);

            List<Pair<JoinDesc, TableKind>> tableKindByJoins = JoinDescUtil.resolveTableType(joins);
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
                            && oldJoinTable.getKind() != newJoinTable.getKind()) {
                        // this case is deprecated, the join table is FACT by default
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

        private static JoinDesc[] getJoinDescHierarchy(JoinsGraph joinsTree, TableRef leaf) {
            if (leaf == null) {
                throw new IllegalStateException("The TableRef cannot be NULL !");
            }

            JoinDesc join = joinsTree.getJoinByPKSide(leaf);
            if (join == null) {
                return new JoinDesc[0];
            }

            return (JoinDesc[]) ArrayUtils.add(getJoinDescHierarchy(joinsTree, join.getFKSide()), join);
        }

        /**
         * get new alias by original table name, for table 'foo'
         * foo -> foo_1
         * foo_1 -> foo_2
         */
        private static String getNewAlias(String originalName, String oldAlias) {
            if (oldAlias.equals(originalName)) {
                return originalName + "_1";
            } else if (!oldAlias.startsWith(originalName + "_")) {
                return originalName;
            }

            String number = oldAlias.substring(originalName.length() + 1);
            try {
                int i = Integer.parseInt(number);
                return originalName + "_" + (i + 1);
            } catch (Exception e) {
                return originalName + "_1";
            }
        }
    }
}
