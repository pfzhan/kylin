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

package io.kyligence.kap.rest.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.security.QueryInterceptor;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.ColumnACLManager;
import io.kyligence.kap.metadata.model.KapModel;

public class ColumnInterceptor extends QueryInterceptor implements IKeep {

    @Override
    protected boolean isEnabled() {
        return KapConfig.getInstanceFromEnv().isColumnACLEnabled();
    }

    @Override
    public Set<String> getQueryIdentifiers(List<OLAPContext> contexts) {
        String project = getProject(contexts);
        return getAllColsWithTblAndSchema(project, contexts);
    }

    @Override
    protected Set<String> getIdentifierBlackList(List<OLAPContext> contexts) {
        String project = getProject(contexts);
        String username = getUser(contexts);

        return ColumnACLManager
                .getInstance(KylinConfig.getInstanceFromEnv())
                .getColumnACLByCache(project)
                .getColumnBlackListByUser(username);
    }

    @Override
    protected String getIdentifierType() {
        return "column";
    }

    protected Set<String> getAllColsWithTblAndSchema(String project, List<OLAPContext> contexts) {
        // all columns with table and DB. Like DB.TABLE.COLUMN
        Set<String> allColWithTblAndSchema = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (OLAPContext context : contexts) {
            for (TblColRef tblColRef : context.allColumns) {
                ColumnDesc columnDesc = tblColRef.getColumnDesc();
                //computed column
                if (columnDesc.isComputedColumn()) {
                    allColWithTblAndSchema.addAll(getCCUsedCols(project, columnDesc));
                }
                //normal column
                allColWithTblAndSchema.add(tblColRef.getColumWithTableAndSchema());
            }
        }
        return allColWithTblAndSchema;
    }

    private Set<String> getCCUsedCols(String project, ColumnDesc columnDesc) {
        Set<String> usedCols = new HashSet<>();
        Map<String, String> aliasTableMap = getAliasTableMap(project, columnDesc.getName());
        Preconditions.checkState(aliasTableMap.size() > 0, "can not find cc:" + columnDesc.getName() + "'s table alias");

        List<Pair<String, String>> colsWithAlias = ExprIdentifierFinder.getExprIdentifiers(columnDesc.getComputedColumnExpr());
        for (Pair<String, String> cols : colsWithAlias) {
            String tableIdentifier = aliasTableMap.get(cols.getFirst());
            usedCols.add(tableIdentifier + "." + cols.getSecond());
        }
        //Preconditions.checkState(usedCols.size() > 0, "can not find cc:" + columnDesc.getName() + "'s used cols");
        return usedCols;
    }

    private Map<String, String> getAliasTableMap(String project, String ccName) {
        DataModelDesc model = getModel(project, ccName);
        Map<String, String> tableWithAlias = new HashMap<>();
        for (String alias : model.getAliasMap().keySet()) {
            String tableName = model.getAliasMap().get(alias).getTableDesc().getIdentity();
            tableWithAlias.put(alias, tableName);
        }
        return tableWithAlias;
    }

    private DataModelDesc getModel(String project, String ccName) {
        List<DataModelDesc> models = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv()).getModels(project);
        for (DataModelDesc modelDesc : models) {
            KapModel model = (KapModel) modelDesc;
            Set<String> computedColumnNames = model.getComputedColumnNames();
            if (computedColumnNames.contains(ccName)) {
                return model;
            }
        }
        return null;
    }

    static class ExprIdentifierFinder extends SqlBasicVisitor<SqlNode> {
        List<Pair<String, String>> columnWithTableAlias;

        ExprIdentifierFinder() {
            this.columnWithTableAlias = new ArrayList<>();
        }

        List<Pair<String, String>> getIdentifiers() {
            return columnWithTableAlias;
        }

        static List<Pair<String, String>> getExprIdentifiers(String expr) {
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

}
