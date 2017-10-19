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
package io.kyligence.kap.query.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.query.schema.OLAPTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// match alias in query to alias in model
// Not designed to reuse, re-new per query
public class QueryAliasMatcher {
    private final static Logger logger = LoggerFactory.getLogger(QueryAliasMatcher.class);

    public final static ColumnRowType SUBQUERY_TAG = new ColumnRowType(null);

    //capture all the join within a SqlSelect's from clause, won't go into any subquery
    class SqlJoinCapturer extends SqlBasicVisitor<SqlNode> {
        private List<JoinDesc> joinDescs;
        private LinkedHashMap<String, ColumnRowType> queryAlias = Maps.newLinkedHashMap(); // aliasInQuery => ColumnRowType representing the alias table

        SqlJoinCapturer() {
            this.joinDescs = new ArrayList<>();
        }

        List<JoinDesc> getJoinDescs() {
            return joinDescs;
        }

        LinkedHashMap<String, ColumnRowType> getQueryAlias() {
            return queryAlias;
        }

        TableRef getFirstTable() {
            if (queryAlias.size() == 0) {
                throw new IllegalStateException("queryAlias map is empty");
            }
            ColumnRowType first = Iterables.getFirst(queryAlias.values(), null);
            Preconditions.checkNotNull(first);
            return first.getAllColumns().get(0).getTableRef();
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {

            if (call instanceof SqlSelect) {
                //don't go into sub-query
                return null;
            }

            // record alias, since the passed in root is a FROM clause, any AS is related to table alias
            if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlAsOperator) {
                //join Table as xxx
                SqlNode[] operands = ((SqlBasicCall) call).getOperands();
                if (operands != null && operands.length == 2) {

                    //both side of the join is SqlIdentifier (not subquery), table as alias
                    if (operands[0] instanceof SqlIdentifier && operands[1] instanceof SqlIdentifier) {
                        String alias = operands[1].toString();
                        SqlIdentifier tableIdentifier = (SqlIdentifier) operands[0];
                        Pair<String, String> schemaAndTable = getSchemaAndTable(tableIdentifier);

                        ColumnRowType columnRowType = buildColumnRowType(alias, schemaAndTable.getFirst(),
                                schemaAndTable.getSecond());
                        queryAlias.put(alias, columnRowType);
                    }

                    //subqeury as alias
                    if ((operands[0] instanceof SqlSelect) || (operands[0] instanceof SqlOrderBy) //
                            && operands[1] instanceof SqlIdentifier) {
                        String alias = operands[1].toString();
                        queryAlias.put(alias, SUBQUERY_TAG);
                    }
                }

                return null; //don't visit child any more
            }

            List<SqlNode> operandList = call.getOperandList();
            // if it's a join, the last element is "condition".
            // skip its part as it may also contain SqlIdentifier(representing condition column), 
            // which is hard to tell from SqlIdentifier representing join tables (without AS)
            List<SqlNode> operands = call instanceof SqlJoin ? operandList.subList(0, operandList.size() - 1)
                    : operandList;
            for (SqlNode operand : operands) {
                if (operand != null) {
                    operand.accept(this);
                }
            }

            //Note this part is after visiting all children, it's on purpose so that by the time the SqlJoin
            //is handled, all of its related join tables must have been recorded in queryAlias
            if (call instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) call;
                if (join.getConditionType() != JoinConditionType.ON) {
                    throw new IllegalArgumentException("JoinConditionType is not ON");
                }
                if (join.getJoinType() != JoinType.INNER && join.getJoinType() != JoinType.LEFT) {
                    throw new IllegalArgumentException("JoinType must be INNER or LEFT");
                }

                if (join.getCondition() instanceof SqlBasicCall) {
                    JoinConditionCapturer joinConditionCapturer = new JoinConditionCapturer(queryAlias,
                            join.getJoinType().toString());
                    join.getCondition().accept(joinConditionCapturer);
                    JoinDesc joinDesc = joinConditionCapturer.getJoinDescs();
                    if (joinDesc.getForeignKey().length != 0)
                        joinDescs.add(joinDesc);
                } else {
                    throw new IllegalArgumentException("join condition should be SqlBasicCall");
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            Pair<String, String> schemaAndTable = getSchemaAndTable(id);
            ColumnRowType columnRowType = buildColumnRowType(schemaAndTable.getSecond(), schemaAndTable.getFirst(),
                    schemaAndTable.getSecond());
            queryAlias.put(schemaAndTable.getSecond(), columnRowType);

            return null;
        }

        private ColumnRowType buildColumnRowType(String alias, String schemaName, String tableName) {
            OLAPTable olapTable = getTable(schemaName, tableName);

            TableRef tableRef = TblColRef.tableForUnknownModel(alias, olapTable.getSourceTable());

            List<TblColRef> columns = new ArrayList<>();
            for (ColumnDesc sourceColumn : olapTable.getSourceColumns()) {
                TblColRef colRef = TblColRef.columnForUnknownModel(tableRef, sourceColumn);
                columns.add(colRef);
            }

            return new ColumnRowType(columns);
        }

        private Pair<String, String> getSchemaAndTable(SqlIdentifier tableIdentifier) {
            String schemaName, tableName;
            if (tableIdentifier.names.size() == 2) {
                schemaName = tableIdentifier.names.get(0);
                tableName = tableIdentifier.names.get(1);
            } else if (tableIdentifier.names.size() == 1) {
                schemaName = defaultSchema;
                tableName = tableIdentifier.names.get(0);
            } else {
                throw new IllegalStateException("table.names size being " + tableIdentifier.names.size());
            }
            return Pair.newPair(schemaName, tableName);
        }
    }

    class JoinConditionCapturer extends SqlBasicVisitor<SqlNode> {
        private final String[] COLUMN_ARRAY_MARKER = new String[0];
        private final LinkedHashMap<String, ColumnRowType> queryAlias;
        private final String joinType;

        private List<TblColRef> pks = Lists.newArrayList();
        private List<TblColRef> fks = Lists.newArrayList();

        JoinConditionCapturer(LinkedHashMap<String, ColumnRowType> queryAlias, String joinType) {
            this.queryAlias = queryAlias;
            this.joinType = joinType;
        }

        public JoinDesc getJoinDescs() {
            List<String> pkNames = Lists.transform(pks, new Function<TblColRef, String>() {
                @Nullable
                @Override
                public String apply(@Nullable TblColRef input) {
                    return input.getName();
                }
            });
            List<String> fkNames = Lists.transform(fks, new Function<TblColRef, String>() {
                @Nullable
                @Override
                public String apply(@Nullable TblColRef input) {
                    return input.getName();
                }
            });

            JoinDesc join = new JoinDesc();
            join.setType(joinType);
            join.setForeignKey(fkNames.toArray(COLUMN_ARRAY_MARKER));
            join.setForeignKeyColumns(fks.toArray(new TblColRef[fks.size()]));
            join.setPrimaryKey(pkNames.toArray(COLUMN_ARRAY_MARKER));
            join.setPrimaryKeyColumns(pks.toArray(new TblColRef[pks.size()]));
            join.sortByFK();
            return join;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if ((call instanceof SqlBasicCall) && (call.getOperator() instanceof SqlBinaryOperator)) {
                if (call.getOperator().getKind() == SqlKind.AND) {
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand != null) {
                            operand.accept(this);
                        }
                    }
                    return null;

                } else if (call.getOperator().getKind() == SqlKind.EQUALS) {
                    if (call.getOperandList().size() == 2 && call.getOperandList().get(0) instanceof SqlIdentifier
                            && call.getOperandList().get(1) instanceof SqlIdentifier) {

                        TblColRef tblColRef0 = QueryAliasMatchInfo
                                .resolveTblColRef((SqlIdentifier) call.getOperandList().get(0), queryAlias);
                        TblColRef tblColRef1 = QueryAliasMatchInfo
                                .resolveTblColRef((SqlIdentifier) call.getOperandList().get(1), queryAlias);

                        if (tblColRef0 == null || tblColRef1 == null) {
                            return null;
                        }

                        String last = Iterables.getLast(queryAlias.keySet());
                        if (tblColRef1.getTableRef().getAlias().equals(last)) {
                            //for most cases
                            pks.add(tblColRef1);
                            fks.add(tblColRef0);
                        } else if (tblColRef0.getTableRef().getAlias().equals(last)) {
                            pks.add(tblColRef0);
                            fks.add(tblColRef1);
                        }
                        return null;
                    }
                }
            }

            throw new IllegalStateException("Join condition is illegal");
        }

    }

    private String project;
    private String defaultSchema;
    private Map<String, OLAPSchema> schemaMap = Maps.newHashMap();
    private Map<String, Map<String, OLAPTable>> schemaTables = Maps.newHashMap();

    public QueryAliasMatcher(String project, String defaultSchema) {
        this.project = project;
        this.defaultSchema = defaultSchema;
    }

    //    public String translateImplicitComputedColumns(DataModelDesc model, String sql) throws SqlParseException {
    //        SqlNode parsed = CalciteParser.parse(sql);
    //        SqlSelectFinder sqlSelectFinder = new SqlSelectFinder();
    //        parsed.accept(sqlSelectFinder);
    //        List<SqlSelect> selects = sqlSelectFinder.getSubqueries();
    //        for (SqlSelect sqlSelect : selects) {
    //            QueryAliasInfo queryAliasInfo = getAliasInfo(sqlSelect, model);
    //        }
    //
    //        return null;
    //    }

    public QueryAliasMatchInfo match(DataModelDesc model, SqlSelect sqlSelect) throws SqlParseException {
        if (sqlSelect.getFrom() == null || sqlSelect.getFrom().getKind().equals(SqlKind.VALUES)) {
            return null;
        }

        SqlSelect subQuery = getSubquery(sqlSelect.getFrom());
        boolean reUseSubqeury = false;

        if (subQuery != null) {
            logger.debug(
                    "Query from a subquery, re-use its QueryAliasMatchInfo only if the subquery is a \"select *\" query");

            if (subQuery.getSelectList().size() == 1 && subQuery.getSelectList().get(0).toString().equals("*")) {
                reUseSubqeury = true;
            } else {
                return null;
            }
        }

        SqlJoinCapturer sqlJoinCapturer = new SqlJoinCapturer();

        if (reUseSubqeury) {
            subQuery.getFrom().accept(sqlJoinCapturer);
        } else {
            sqlSelect.getFrom().accept(sqlJoinCapturer);
        }

        LinkedHashMap<String, ColumnRowType> queryAlias = sqlJoinCapturer.getQueryAlias();

        List<JoinDesc> joinDescs = sqlJoinCapturer.getJoinDescs();
        TableRef firstTable = sqlJoinCapturer.getFirstTable();
        JoinsTree joinsTree = new JoinsTree(firstTable, joinDescs);
        Map<String, String> matches = joinsTree.matches(model.getJoinsTree());
        if (matches == null || matches.isEmpty()) {
            return null;
        }
        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.putAll(matches);

        return new QueryAliasMatchInfo(aliasMapping, queryAlias);
    }

    private SqlSelect getSubquery(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            return (SqlSelect) sqlNode;
        } else if (sqlNode.getKind().equals(SqlKind.UNION)) {
            return (SqlSelect) ((SqlBasicCall) sqlNode).getOperandList().get(0);
        } else if (sqlNode.getKind().equals(SqlKind.AS)) {
            return getSubquery(((SqlBasicCall) sqlNode).getOperandList().get(0));
        }

        return null;
    }

    private OLAPSchema getSchema(String name) {
        OLAPSchema olapSchema = schemaMap.get(name);
        if (olapSchema == null) {
            olapSchema = new OLAPSchema(project, name, true);
            schemaMap.put(name, olapSchema);
        }
        return olapSchema;
    }

    private OLAPTable getTable(String schemaName, String tableName) {
        Map<String, OLAPTable> localTables = schemaTables.get(schemaName);
        if (localTables == null) {
            OLAPSchema olapSchema = getSchema(schemaName);
            localTables = Maps.newHashMap();
            for (Map.Entry<String, Table> entry : olapSchema.getTableMap().entrySet()) {
                localTables.put(entry.getKey(), (OLAPTable) entry.getValue());
            }
            schemaTables.put(schemaName, localTables);
        }
        return localTables.get(tableName);
    }

}
