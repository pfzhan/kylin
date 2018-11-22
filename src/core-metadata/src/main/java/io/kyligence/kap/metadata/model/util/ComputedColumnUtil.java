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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

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

    public static Set<String> getCCUsedColsWithProject(String project, ColumnDesc columnDesc) {
        NDataModel model = getModel(project, columnDesc.getName());
        Set<String> usedCols = getCCUsedColsWithModel(model, columnDesc);
        return usedCols;
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

    private static Set<String> getCCUsedCols(NDataModel model, String colName, String ccExpr) {
        Set<String> usedCols = new HashSet<>();
        Map<String, String> aliasTableMap = getAliasTableMap(model);
        Preconditions.checkState(aliasTableMap.size() > 0, "can not find cc:" + colName + "'s table alias");
        List<Pair<String, String>> colsWithAlias = ExprIdentifierFinder.getExprIdentifiers(ccExpr);
        for (Pair<String, String> cols : colsWithAlias) {
            String tableIdentifier = aliasTableMap.get(cols.getFirst());
            usedCols.add(tableIdentifier + "." + cols.getSecond());
        }
        //Preconditions.checkState(usedCols.size() > 0, "can not find cc:" + columnDesc.getName() + "'s used cols");
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
        List<NDataModel> models = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataModels();
        for (NDataModel modelDesc : models) {
            NDataModel model = modelDesc;
            Set<String> computedColumnNames = model.getComputedColumnNames();
            if (computedColumnNames.contains(ccName)) {
                return model;
            }
        }
        return null;
    }
}
