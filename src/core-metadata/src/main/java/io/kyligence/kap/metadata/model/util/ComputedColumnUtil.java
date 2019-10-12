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
package io.kyligence.kap.metadata.model.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.alias.AliasDeduce;
import io.kyligence.kap.metadata.model.alias.AliasMapping;
import io.kyligence.kap.metadata.model.alias.ExpressionComparator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;

public class ComputedColumnUtil {
    public static class ExprIdentifierFinder extends SqlBasicVisitor<SqlNode> {
        List<Pair<String, String>> columnWithTableAlias;

        ExprIdentifierFinder() {
            this.columnWithTableAlias = new ArrayList<>();
        }

        List<Pair<String, String>> getIdentifiers() {
            return columnWithTableAlias;
        }

        public static List<Pair<String, String>> getExprIdentifiers(String expr) {
            SqlNode exprNode = CalciteParser.getExpNode(expr);
            ExprIdentifierFinder id = new ExprIdentifierFinder();
            exprNode.accept(id);
            return id.getIdentifiers();
        }

        @Override
        public SqlNode visit(SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            //Preconditions.checkState(id.names.size() == 2, "error when get identifier in cc's expr");
            if (id.names.size() == 2) {
                columnWithTableAlias.add(Pair.newPair(id.names.get(0), id.names.get(1)));
            }
            return null;
        }
    }

    public static Map<String, Set<String>> getCCUsedColsMapWithProject(String project, ColumnDesc columnDesc) {
        return getCCUsedColsMapWithModel(getModel(project, columnDesc.getName()), columnDesc);
    }

    public static Set<String> getCCUsedColsWithProject(String project, ColumnDesc columnDesc) {
        NDataModel model = getModel(project, columnDesc.getName());
        Set<String> usedCols = getCCUsedColsWithModel(model, columnDesc);
        return usedCols;
    }

    public static Map<String, Set<String>> getCCUsedColsMapWithModel(NDataModel model, ColumnDesc columnDesc) {
        return getCCUsedColsMap(model, columnDesc.getName(), columnDesc.getComputedColumnExpr());
    }

    public static Set<String> getCCUsedColsWithModel(NDataModel model, ColumnDesc columnDesc) {
        return getCCUsedCols(model, columnDesc.getName(), columnDesc.getComputedColumnExpr());
    }

    public static Set<String> getCCUsedColsWithModel(NDataModel model, ComputedColumnDesc ccDesc) {
        return getCCUsedCols(model, ccDesc.getColumnName(), ccDesc.getExpression());
    }

    public static Set<String> getAllCCUsedColsInModel(NDataModel dataModel) {
        Set<String> ccUsedColsInModel = new HashSet<>();
        NDataModel kapModel = dataModel;
        List<ComputedColumnDesc> ccDescs = kapModel.getComputedColumnDescs();
        for (ComputedColumnDesc ccDesc : ccDescs) {
            ccUsedColsInModel.addAll(ComputedColumnUtil.getCCUsedColsWithModel(dataModel, ccDesc));
        }
        return ccUsedColsInModel;
    }

    public static ColumnDesc[] createComputedColumns(List<ComputedColumnDesc> computedColumnDescs,
            final TableDesc tableDesc) {
        final MutableInt id = new MutableInt(tableDesc.getColumnCount());
        return computedColumnDescs.stream()
                .filter(input -> tableDesc.getIdentity().equalsIgnoreCase(input.getTableIdentity())).map(input -> {
                    id.increment();
                    ColumnDesc columnDesc = new ColumnDesc(id.toString(), input.getColumnName(), input.getDatatype(),
                            input.getComment(), null, null, input.getInnerExpression());
                    columnDesc.init(tableDesc);
                    return columnDesc;
                }).toArray(ColumnDesc[]::new);
    }

    private static Map<String, Set<String>> getCCUsedColsMap(NDataModel model, String colName, String ccExpr) {
        Map<String, Set<String>> usedCols = Maps.newHashMap();
        Map<String, String> aliasTableMap = getAliasTableMap(model);
        Preconditions.checkState(aliasTableMap.size() > 0, "can not find cc:" + colName + "'s table alias");
        List<Pair<String, String>> colsWithAlias = ExprIdentifierFinder.getExprIdentifiers(ccExpr);
        for (Pair<String, String> cols : colsWithAlias) {
            String tableIdentifier = aliasTableMap.get(cols.getFirst());
            if (!usedCols.containsKey(tableIdentifier)) {
                usedCols.put(tableIdentifier, Sets.newHashSet());
            }
            usedCols.get(tableIdentifier).add(cols.getSecond());
        }
        return usedCols;
    }

    private static Set<String> getCCUsedCols(NDataModel model, String colName, String ccExpr) {
        Set<String> usedCols = new HashSet<>();
        Map<String, String> aliasTableMap = getAliasTableMap(model);
        Preconditions.checkState(aliasTableMap.size() > 0, "can not find cc:" + colName + "'s table alias");
        List<Pair<String, String>> colsWithAlias = ExprIdentifierFinder.getExprIdentifiers(ccExpr);
        for (Pair<String, String> cols : colsWithAlias) {
            String tableIdentifier = aliasTableMap.get(cols.getFirst());
            usedCols.add(tableIdentifier + "." + cols.getSecond());
        }
        //Preconditions.checkState(usedCols.size() > 0, "can not find cc:" + columnDesc.getUuid() + "'s used cols");
        return usedCols;
    }

    private static Map<String, String> getAliasTableMap(NDataModel model) {
        Map<String, String> tableWithAlias = new HashMap<>();
        for (String alias : model.getAliasMap().keySet()) {
            String tableName = model.getAliasMap().get(alias).getTableDesc().getIdentity();
            tableWithAlias.put(alias, tableName);
        }
        return tableWithAlias;
    }

    private static NDataModel getModel(String project, String ccName) {
        List<NDataModel> models = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listUnderliningDataModels();
        for (NDataModel modelDesc : models) {
            NDataModel model = modelDesc;
            Set<String> computedColumnNames = model.getComputedColumnNames();
            if (computedColumnNames.contains(ccName)) {
                return model;
            }
        }
        return null;
    }

    public static void singleCCConflictCheck(NDataModel existingModel, NDataModel newModel,
                                             ComputedColumnDesc existingCC, ComputedColumnDesc newCC, CCConflictHandler handler) {
        AliasMapping aliasMapping = getCCAliasMapping(existingModel, newModel, existingCC, newCC);
        boolean sameName = StringUtils.equalsIgnoreCase(
                existingCC.getTableIdentity() + "." + existingCC.getColumnName(),
                newCC.getTableIdentity() + "." + newCC.getColumnName());
        boolean sameCCExpr = isSameCCExpr(existingCC, newCC, aliasMapping);

        if (sameName) {
            if (!isSameAliasTable(existingCC, newCC, aliasMapping)) {
                handler.handleOnWrongPositionName(existingModel, existingCC, newCC, aliasMapping);
            }

            if (!sameCCExpr) {
                handler.handleOnSameNameDiffExpr(existingModel, newModel, existingCC, newCC);
            }
        }

        if (sameCCExpr) {
            if (!isSameAliasTable(existingCC, newCC, aliasMapping)) {
                handler.handleOnWrongPositionExpr(existingModel, existingCC, newCC, aliasMapping);
            }

            if (!sameName) {
                handler.handleOnSameExprDiffName(existingModel, existingCC, newCC);
            }
        }
    }

    private static AliasMapping getAliasMappingFromJoinsGraph(JoinsGraph fromGraph, JoinsGraph toMatchGraph) {
        AliasMapping adviceAliasMapping = null;

        Map<String, String> matches = fromGraph.matchAlias(toMatchGraph, true);
        if (matches != null && !matches.isEmpty()) {
            BiMap<String, String> biMap = HashBiMap.create();
            biMap.putAll(matches);
            adviceAliasMapping = new AliasMapping(biMap);
        }
        return adviceAliasMapping;
    }

    private static AliasMapping getCCAliasMapping(NDataModel existingModel, NDataModel newModel,
                                                  ComputedColumnDesc existingCC, ComputedColumnDesc newCC) {
        JoinsGraph newCCGraph = getCCExprRelatedSubgraph(newCC, newModel);
        JoinsGraph existCCGraph = getCCExprRelatedSubgraph(existingCC, existingModel);
        return getAliasMappingFromJoinsGraph(newCCGraph, existCCGraph);
    }

    // model X contains table f,a,b,c, and model Y contains table f,a,b,d
    // if two cc involve table a,b, they might still be treated equal regardless of the model difference on c,d
    private static JoinsGraph getCCExprRelatedSubgraph(ComputedColumnDesc cc, NDataModel model) {
        Set<String> aliasSets = getUsedAliasSet(cc.getExpression());
        if (cc.getTableAlias() != null) {
            aliasSets.add(cc.getTableAlias());
        }
        return model.getJoinsGraph().getSubgraphByAlias(aliasSets);
    }

    public static Set<String> getUsedAliasSet(String expr) {
        if (expr == null) {
            return Sets.newHashSet();
        }
        SqlNode sqlNode = CalciteParser.getExpNode(expr);

        final Set<String> s = Sets.newHashSet();
        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                Preconditions.checkState(id.names.size() == 2);
                s.add(id.names.get(0));
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        return s;
    }

    public static boolean isLiteralSameCCExpr(ComputedColumnDesc existingCC, ComputedColumnDesc newCC) {
        String definition0 = existingCC.getExpression();
        String definition1 = newCC.getExpression();

        if (definition0 == null) {
            return definition1 == null;
        } else if (definition1 == null) {
            return false;
        }

        definition0 = definition0.replaceAll("\\s*", "");
        definition1 = definition1.replaceAll("\\s*", "");
        return definition0.equalsIgnoreCase(definition1);
    }

    private static boolean isSameCCExpr(ComputedColumnDesc existingCC, ComputedColumnDesc newCC,
                                        AliasMapping aliasMapping) {
        if (existingCC.getExpression() == null) {
            return newCC.getExpression() == null;
        } else if (newCC.getExpression() == null) {
            return false;
        }

        return ExpressionComparator.isNodeEqual(CalciteParser.getExpNode(newCC.getExpression()),
                CalciteParser.getExpNode(existingCC.getExpression()), aliasMapping, AliasDeduce.NO_OP);
    }

    private static boolean isSameAliasTable(ComputedColumnDesc existingCC, ComputedColumnDesc newCC,
                                            AliasMapping adviceAliasMapping) {
        if (adviceAliasMapping == null) {
            return false;
        }
        String existingAlias = existingCC.getTableAlias();
        String newAlias = newCC.getTableAlias();
        return StringUtils.equals(newAlias, adviceAliasMapping.getAliasMapping().get(existingAlias));
    }

    public interface CCConflictHandler {
        void handleOnWrongPositionName(NDataModel existingModel, ComputedColumnDesc existingCC,
                                       ComputedColumnDesc newCC, AliasMapping positionAliasMapping);

        void handleOnSameNameDiffExpr(NDataModel existingModel, NDataModel newModel, ComputedColumnDesc existingCC,
                                      ComputedColumnDesc newCC);

        void handleOnWrongPositionExpr(NDataModel existingModel, ComputedColumnDesc existingCC,
                                       ComputedColumnDesc newCC, AliasMapping positionAliasMapping);

        void handleOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
                                      ComputedColumnDesc newCC);
    }

    public static class DefaultCCConflictHandler implements CCConflictHandler {

        @Override
        public void handleOnSameNameDiffExpr(NDataModel existingModel, NDataModel newModel,
                                             ComputedColumnDesc existingCC, ComputedColumnDesc newCC) {
            JoinsGraph ccJoinsGraph = getCCExprRelatedSubgraph(existingCC, existingModel);
            AliasMapping aliasMapping = getAliasMappingFromJoinsGraph(ccJoinsGraph, newModel.getJoinsGraph());
            String advisedExpr = aliasMapping == null ? null
                    : CalciteParser.replaceAliasInExpr(existingCC.getExpression(), aliasMapping.getAliasMapping());

            String msg = String.format(
                    "Column name for computed column %s is already used in model %s, you should apply the same expression %s here, or use a different computed column name.",
                    newCC.getFullName(), existingModel.getAlias(), advisedExpr != null ? "as \' " + advisedExpr + " \'"
                            : "like \' " + existingCC.getExpression() + " \'");
            throw new BadModelException(msg, BadModelException.CauseType.SAME_NAME_DIFF_EXPR, advisedExpr,
                    existingModel.getAlias(), newCC.getFullName());
        }

        @Override
        public void handleOnWrongPositionName(NDataModel existingModel, ComputedColumnDesc existingCC,
                                              ComputedColumnDesc newCC, AliasMapping positionAliasMapping) {
            String advice = positionAliasMapping == null ? null
                    : positionAliasMapping.getAliasMapping().get(existingCC.getTableAlias());

            String msg = null;

            if (advice != null) {
                msg = String.format(
                        "Computed column %s is already defined in model %s, to reuse it you have to define it on alias table: %s",
                        newCC.getColumnName(), existingModel.getAlias(), advice);
            } else {
                msg = String.format(
                        "Computed column %s is already defined in model %s, no suggestion could be provided to reuse it",
                        newCC.getColumnName(), existingModel.getAlias());
            }

            throw new BadModelException(msg, BadModelException.CauseType.WRONG_POSITION_DUE_TO_NAME, advice,
                    existingModel.getAlias(), newCC.getFullName());
        }

        @Override
        public void handleOnWrongPositionExpr(NDataModel existingModel, ComputedColumnDesc existingCC,
                                              ComputedColumnDesc newCC, AliasMapping positionAliasMapping) {
            String advice = positionAliasMapping == null ? null
                    : positionAliasMapping.getAliasMapping().get(existingCC.getTableAlias());

            String msg = null;

            if (advice != null) {
                msg = String.format(
                        "Computed column %s's expression is already defined in model %s, to reuse it you have to define it on alias table: %s",
                        newCC.getColumnName(), existingModel.getAlias(), advice);
            } else {
                msg = String.format(
                        "Computed column %s's expression is already defined in model %s, no suggestion could be provided to reuse it",
                        newCC.getColumnName(), existingModel.getAlias());
            }

            throw new BadModelException(msg, BadModelException.CauseType.WRONG_POSITION_DUE_TO_EXPR, advice,
                    existingModel.getAlias(), newCC.getFullName());
        }

        @Override
        public void handleOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
                                             ComputedColumnDesc newCC) {
            String adviseName = existingCC.getColumnName();
            String msg = String.format(
                    "Expression %s in computed column %s is already defined by computed column %s from model %s, you should use the same column name: ' %s ' .",
                    newCC.getExpression(), newCC.getFullName(), existingCC.getFullName(), existingModel.getAlias(),
                    existingCC.getColumnName());
            throw new BadModelException(msg, BadModelException.CauseType.SAME_EXPR_DIFF_NAME, adviseName,
                    existingModel.getAlias(), newCC.getFullName());
        }
    }

    public static List<Pair<ComputedColumnDesc, NDataModel>> getExistingCCs(String modelId, List<NDataModel> otherModels) {
        List<Pair<ComputedColumnDesc, NDataModel>> existingCCs = Lists.newArrayList();
        for (NDataModel otherModel : otherModels) {
            if (!StringUtils.equals(otherModel.getUuid(), modelId)) {
                for (ComputedColumnDesc cc : otherModel.getComputedColumnDescs()) {
                    existingCCs.add(Pair.newPair(cc, otherModel));
                }
            }
        }
        return existingCCs;
    }
}
