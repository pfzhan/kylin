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

package io.kyligence.kap.query.engine.mask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.parser.ParseException;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.acl.DependentColumn;
import io.kyligence.kap.metadata.acl.DependentColumnInfo;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.relnode.KapTableScan;

public class QueryDependentColumnMask implements QueryResultMask {

    private RelNode rootRelNode;

    private String defaultDatabase;

    private DependentColumnInfo dependentInfo;

    private List<ResultColumnMaskInfo> resultColumnMaskInfos;
    private boolean needMask = false;

    public QueryDependentColumnMask(String project, KylinConfig kylinConfig) {
        defaultDatabase = NProjectManager.getInstance(kylinConfig).getProject(project).getDefaultDatabase();
        QueryContext.AclInfo aclInfo = QueryContext.current().getAclInfo();
        if (aclInfo != null) {
            dependentInfo = AclTCRManager.getInstance(kylinConfig, project).getDependentColumns(aclInfo.getUsername(),
                    aclInfo.getGroups());
        }
    }

    public QueryDependentColumnMask(String defaultDatabase, DependentColumnInfo dependentInfo) {
        this.defaultDatabase = defaultDatabase;
        this.dependentInfo = dependentInfo;
    }

    public void doSetRootRelNode(RelNode relNode) {
        this.rootRelNode = relNode;
    }

    public void init() {
        assert rootRelNode != null;
        resultColumnMaskInfos = buildResultColumnMaskInfo(getRefCols(rootRelNode));
        for (ResultColumnMaskInfo resultColumnMaskInfo : resultColumnMaskInfos) {
            if (resultColumnMaskInfo.needMask()) {
                needMask = true;
                break;
            }
        }
    }

    public Dataset<Row> doMaskResult(Dataset<Row> df) {
        if (dependentInfo == null || rootRelNode == null || !dependentInfo.needMask()) {
            return df;
        }
        if (resultColumnMaskInfos == null) {
            init();
        }
        if (!needMask) {
            return df;
        }

        return doResultMaskInternal(df);
    }

    private Dataset<Row> doResultMaskInternal(Dataset<Row> df) {
        Column[] columns = new Column[df.columns().length];
        Dataset<Row> dfWithIndexedCol = MaskUtil.dFToDFWithIndexedColumns(df);
        for (int i = 0; i < dfWithIndexedCol.columns().length; i++) {
            ResultColumnMaskInfo maskInfo = resultColumnMaskInfos.get(i);
            if (!maskInfo.needMask()) {
                columns[i] = dfWithIndexedCol.col(dfWithIndexedCol.columns()[i]);
            } else if (maskInfo.maskAsNull) {
                columns[i] = new Column(new Literal(null, dfWithIndexedCol.schema().fields()[i].dataType()))
                        .as(dfWithIndexedCol.columns()[i]);
            } else {
                try {
                    String condExpr = maskDependentCondition(dfWithIndexedCol, maskInfo);
                    Expression expr = dfWithIndexedCol.sparkSession().sessionState().sqlParser()
                            .parseExpression(String.format(Locale.ROOT, "CASE WHEN (%s) THEN `%s` ELSE NULL END",
                                    condExpr, dfWithIndexedCol.columns()[i]));
                    columns[i] = new Column(expr).as(dfWithIndexedCol.columns()[i]);
                } catch (ParseException e) {
                    throw new KylinException(ServerErrorCode.ACL_DEPENDENT_COLUMN_PARSE_ERROR, e);
                }
            }
        }
        return dfWithIndexedCol.select(columns).toDF(df.columns());
    }

    private String maskDependentCondition(Dataset<Row> dfWithIndexedCol, ResultColumnMaskInfo maskInfo) {
        StringBuilder condExpr = new StringBuilder();
        for (ResultDependentValues dependentValue : maskInfo.dependentValues) {
            String depColumnName = dfWithIndexedCol.columns()[dependentValue.colIdx];
            if (condExpr.length() > 0) {
                condExpr.append(" AND ");
            }
            condExpr.append("(");
            condExpr.append("`").append(depColumnName).append("`");
            condExpr.append(" IN (");
            boolean firstVal = true;
            for (String depValue : dependentValue.values) {
                if (!firstVal) {
                    condExpr.append(",");
                }
                condExpr.append("'").append(depValue).append("'");
                firstVal = false;
            }
            condExpr.append("))");
        }
        return condExpr.toString();
    }

    private List<ResultColumnMaskInfo> buildResultColumnMaskInfo(List<ColumnReferences> resultColRefs) {
        // map of simple projected single column to its idx in the result
        HashMap<String, Integer> simpleProjectColumnMap = new HashMap<>();
        int i = 0;
        for (ColumnReferences ref : resultColRefs) {
            if (ref.isSimpleSingleColumnProject()) {
                simpleProjectColumnMap.put(ref.references.iterator().next(), i);
            }
            i++;
        }

        List<ResultColumnMaskInfo> resultMaskInfos = new LinkedList<>();
        for (ColumnReferences resultColRef : resultColRefs) {
            ResultColumnMaskInfo maskInfo = new ResultColumnMaskInfo();

            // search all cols used by current result column
            for (String referenceId : resultColRef.references) {
                // if has no dep columns, do not do any mask
                Collection<DependentColumn> dependentColumns = dependentInfo.get(referenceId);
                if (dependentColumns.isEmpty()) {
                    continue;
                }

                for (DependentColumn dependentColumn : dependentColumns) {
                    Integer depIdx = simpleProjectColumnMap.get(dependentColumn.getDependentColumnIdentity());

                    // if no simple project dependent col ref is found, mask as null
                    if (depIdx == null) {
                        maskInfo.maskAsNull = true;
                        break;
                    }

                    maskInfo.addDependentValues(
                            new ResultDependentValues(depIdx, dependentColumn.getDependentValues()));
                }
            }

            resultMaskInfos.add(maskInfo);
        }
        return resultMaskInfos;
    }

    /**
     * Search relNodes from bottom-up, and collect all the col refs
     *
     * @param relNode
     * @return
     */
    private List<ColumnReferences> getRefCols(RelNode relNode) {
        if (relNode instanceof TableScan) {
            return getTableColRefs((TableScan) relNode);
        } else if (relNode instanceof Values) {
            return relNode.getRowType().getFieldList().stream().map(f -> new ColumnReferences())
                    .collect(Collectors.toList());
        } else if (relNode instanceof Aggregate) {
            return getAggregateColRefs((Aggregate) relNode);
        } else if (relNode instanceof Project) {
            return getProjectColRefs((Project) relNode);
        } else if (relNode instanceof SetOp) {
            return getUnionColRefs((SetOp) relNode);
        } else if (relNode instanceof Window) {
            return getWindowColRefs((Window) relNode);
        } else {
            List<ColumnReferences> refs = new LinkedList<>();
            for (RelNode input : relNode.getInputs()) {
                refs.addAll(getRefCols(input));
            }
            return refs;
        }
    }

    private List<ColumnReferences> getWindowColRefs(Window window) {
        List<ColumnReferences> inputRefs = getRefCols(window.getInput(0));
        List<ColumnReferences> colRefs = new LinkedList<>(inputRefs);
        List<RexNode> aggCalls = window.groups.stream().flatMap(group -> group.aggCalls.stream())
                .collect(Collectors.toList());
        for (RexNode aggCall : aggCalls) {
            ColumnReferences ref = new ColumnReferences();
            for (Integer bit : RelOptUtil.InputFinder.bits(aggCall)) {
                if (bit < inputRefs.size() && inputRefs.get(bit) != null) { // skip constants
                    ref = ref.merge(inputRefs.get(bit));
                }
            }
            colRefs.add(ref);
        }
        return colRefs;
    }

    private List<ColumnReferences> getUnionColRefs(SetOp setOp) {
        List<ColumnReferences> refs = new LinkedList<>();
        for (RelNode input : setOp.getInputs()) {
            List<ColumnReferences> inputRefs = getRefCols(input);
            if (refs.isEmpty()) {
                refs = inputRefs;
            } else {
                for (int i = 0; i < inputRefs.size(); i++) {
                    refs.set(i, refs.get(i).merge(inputRefs.get(i)));
                }
            }
        }
        return refs;
    }

    private List<ColumnReferences> getProjectColRefs(Project project) {
        List<ColumnReferences> inputRefs = getRefCols(project.getInput(0));
        List<ColumnReferences> refs = new LinkedList<>();
        for (RexNode expr : project.getChildExps()) {
            ColumnReferences ref = new ColumnReferences();
            for (Integer input : RelOptUtil.InputFinder.bits(expr)) {
                ref = ref.merge(inputRefs.get(input));
            }
            // if it's not a direct input ref, the expr then has some calculations
            if (!(expr instanceof RexInputRef)) {
                ref.hasCalculation = true;
            }
            refs.add(ref);
        }
        return refs;
    }

    private List<ColumnReferences> getAggregateColRefs(Aggregate aggregate) {
        List<ColumnReferences> inputRefs = getRefCols(aggregate.getInput(0));
        List<ColumnReferences> refs = new LinkedList<>();
        for (Integer groupInputIdx : aggregate.getGroupSet()) {
            refs.add(inputRefs.get(groupInputIdx));
        }
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            ColumnReferences ref = new ColumnReferences();
            for (Integer argInputIdx : aggregateCall.getArgList()) {
                ref = ref.merge(inputRefs.get(argInputIdx));
            }
            // the col refs are aggregated
            ref.hasAggregation = true;
            refs.add(ref);
        }
        return refs;
    }

    /**
     * get refs of all columns on table, including computed columns
     *
     * @param tableScan
     * @return
     */
    private List<ColumnReferences> getTableColRefs(TableScan tableScan) {
        assert tableScan.getTable().getQualifiedName().size() == 2;
        String dbName = tableScan.getTable().getQualifiedName().get(0);
        String tableName = tableScan.getTable().getQualifiedName().get(1);
        List<ColumnReferences> refs = new ArrayList<>();
        for (RelDataTypeField field : tableScan.getRowType().getFieldList()) {
            ColumnDesc columnDesc = ((KapTableScan) tableScan).getOlapTable().getSourceColumns().get(field.getIndex());
            if (columnDesc.isComputedColumn()) {
                refs.add(getCCReferences(columnDesc.getComputedColumnExpr()));
            } else {
                refs.add(new ColumnReferences(dbName + "." + tableName + "." + field.getName()));
            }
        }
        return refs;
    }

    /**
     * parse cc expr, extract sql identifiers and search identifiers' mask in maskInfo
     *
     * @param ccExpr
     * @return
     */
    private ColumnReferences getCCReferences(String ccExpr) {
        ColumnReferences columnReferences = new ColumnReferences();
        List<SqlIdentifier> ids = MaskUtil.getCCCols(ccExpr);
        for (SqlIdentifier id : ids) {
            if (id.names.size() == 2) {
                columnReferences.addReference(defaultDatabase + "." + id.toString());
            } else if (id.names.size() == 3) {
                columnReferences.addReference(id.toString());
            }
        }
        columnReferences.hasCalculation = true;
        return columnReferences;
    }

    public List<ResultColumnMaskInfo> getResultColumnMaskInfos() {
        return resultColumnMaskInfos;
    }

    static class ColumnReferences {
        private Set<String> references = new HashSet<>();

        boolean hasCalculation = false;

        boolean hasAggregation = false;

        public ColumnReferences() {
        }

        public ColumnReferences(String column) {
            this.references = Sets.newHashSet(column);
        }

        void addReference(String column) {
            references.add(column);
        }

        void addReferences(Collection<String> columns) {
            references.addAll(columns);
        }

        ColumnReferences merge(ColumnReferences other) {
            if (other == null) {
                return this;
            }
            ColumnReferences ref = new ColumnReferences();
            ref.addReferences(this.references);
            ref.addReferences(other.references);
            ref.hasCalculation = this.hasCalculation || other.hasCalculation;
            ref.hasAggregation = this.hasAggregation || other.hasAggregation;
            return ref;
        }

        boolean isSimpleSingleColumnProject() {
            return references.size() == 1 && !hasAggregation && !hasCalculation;
        }
    }

    static class ResultColumnMaskInfo {

        boolean maskAsNull = false;

        List<ResultDependentValues> dependentValues = new LinkedList<>();

        boolean needMask() {
            return !dependentValues.isEmpty() || maskAsNull;
        }

        void addDependentValues(ResultDependentValues values) {
            dependentValues.add(values);
        }
    }

    static class ResultDependentValues {
        int colIdx;
        Set<String> values;

        public ResultDependentValues(int colIdx, String[] values) {
            this.colIdx = colIdx;
            this.values = Sets.newHashSet(values);
        }
    }

}
