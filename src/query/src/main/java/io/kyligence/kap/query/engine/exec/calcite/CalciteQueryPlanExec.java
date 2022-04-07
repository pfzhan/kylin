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

package io.kyligence.kap.query.engine.exec.calcite;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.util.DateFormat;

import io.kyligence.kap.query.engine.exec.QueryPlanExec;
import io.kyligence.kap.query.engine.meta.MutableDataContext;
import io.kyligence.kap.query.relnode.KapRel;

/**
 * implement and execute a physical plan with Calcite
 * this exec is only used for constants queries
 */
public class CalciteQueryPlanExec implements QueryPlanExec {

    @Override
    public List<List<String>> execute(RelNode rel, MutableDataContext dataContext) {
        QueryContext.currentTrace().startSpan(QueryTrace.EXECUTION);
        initContextVars(dataContext);
        // allocate the olapContext anyway since it's being checked by some unit tests
        new KapRel.OLAPContextImplementor().allocateContext((KapRel) rel.getInput(0), rel);

        List<List<String>> result = doExecute(rel, dataContext);

        //constant query should fill empty list for scan data
        QueryContext.fillEmptyResultSetMetrics();
        QueryContext.currentTrace().endLastSpan();
        return result;
    }

    public List<List<String>> doExecute(RelNode rel, DataContext dataContext) {
        Bindable bindable = EnumerableInterpretable.toBindable(new HashMap<>(), new TrivialSparkHandler(),
                (EnumerableRel) rel, EnumerableRel.Prefer.ARRAY);

        Enumerable<Object> rawResult = bindable.bind(dataContext);
        List<List<String>> result = new LinkedList<>();

        QueryContext.currentTrace().startSpan(QueryTrace.FETCH_RESULT);
        for (Object rawRow : rawResult.toList()) {
            List<String> row = new LinkedList<>();
            if (rel.getRowType().getFieldCount() > 1) {
                Object[] rowData = (Object[]) rawRow;
                for (int i = 0; i < rowData.length; i++) {
                    row.add(rawQueryResultToString(rowData[i], rel.getRowType().getFieldList().get(i).getType()));
                }
            } else {
                row.add(rawQueryResultToString(rawRow, rel.getRowType().getFieldList().get(0).getType()));
            }
            result.add(row);
        }

        return result;
    }

    private void initContextVars(MutableDataContext dataContext) {
        TimeZone timezone = DataContext.Variable.TIME_ZONE.get(dataContext);
        final long time = System.currentTimeMillis();
        final long localOffset = timezone.getOffset(System.currentTimeMillis());
        dataContext.putContextVar(DataContext.Variable.UTC_TIMESTAMP.camelName, time);
        // to align with calcite implementation, current_timestamp = local_timestamp
        // see org.apache.calcite.jdbc.CalciteConnectionImpl.DataContextImpl.DataContextImpl
        dataContext.putContextVar(DataContext.Variable.CURRENT_TIMESTAMP.camelName, time + localOffset);
        dataContext.putContextVar(DataContext.Variable.LOCAL_TIMESTAMP.camelName, time + localOffset);
    }

    // may induce some puzzle result
    private String rawQueryResultToString(Object object, RelDataType dataType) {
        String value = String.valueOf(object);
        switch (dataType.getSqlTypeName()) {
        case DATE:
            return DateFormat.formatDayToEpochToDateStr(Long.parseLong(value), TimeZone.getTimeZone("GMT"));
        case TIMESTAMP:
            return DateFormat.castTimestampToString(Long.parseLong(value), TimeZone.getTimeZone("GMT"));
        default:
            return value;
        }
    }

    private static class TrivialSparkHandler implements CalcitePrepare.SparkHandler {
        public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel, boolean restructure) {
            return rootRel;
        }

        public void registerRules(RuleSetBuilder builder) {
            // This is a trivial implementation. This method might be called but it is not supposed to do anything
        }

        public boolean enabled() {
            return false;
        }

        public ArrayBindable compile(ClassDeclaration expr, String s) {
            throw new UnsupportedOperationException();
        }

        public Object sparkContext() {
            throw new UnsupportedOperationException();
        }
    }
}
