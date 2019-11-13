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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.relnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.query.routing.RealizationCheck;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.storage.StorageContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.relnode.KapRel;
import lombok.Getter;
import lombok.Setter;

/**
 */
public class OLAPContext {

    public static final String PRM_ACCEPT_PARTIAL_RESULT = "AcceptPartialResult";
    public static final String PRM_USER_AUTHEN_INFO = "UserAuthenInfo";
    public static final String PRM_PROJECT_PERMISSION = "ProjectPermission";

    public static final String HAS_ADMIN_PERMISSION = "HasAdminPermission";
    public static final String HAS_EMPTY_PERMISSION = "";

    static final ThreadLocal<Map<String, String>> _localPrarameters = new ThreadLocal<Map<String, String>>();

    static final ThreadLocal<Map<Integer, OLAPContext>> _localContexts = new ThreadLocal<Map<Integer, OLAPContext>>();

    public static void setParameters(Map<String, String> parameters) {
        _localPrarameters.set(parameters);
    }

    public static void clearParameter() {
        _localPrarameters.remove();
    }

    public static void registerContext(OLAPContext ctx) {
        if (_localContexts.get() == null) {
            Map<Integer, OLAPContext> contextMap = new HashMap<Integer, OLAPContext>();
            _localContexts.set(contextMap);
        }
        _localContexts.get().put(ctx.id, ctx);
    }

    public static Collection<OLAPContext> getThreadLocalContexts() {
        Map<Integer, OLAPContext> map = _localContexts.get();
        return map == null ? null : map.values();
    }

    public static OLAPContext getThreadLocalContextById(int id) {
        Map<Integer, OLAPContext> map = _localContexts.get();
        return map.get(id);
    }

    public static void clearThreadLocalContexts() {
        _localContexts.remove();
    }

    public static void clearThreadLocalContextById(int id) {
        Map<Integer, OLAPContext> map = _localContexts.get();
        map.remove(id);
        _localContexts.set(map);
    }

    public OLAPContext(int seq) {
        this.id = seq;
        this.storageContext = new StorageContext(seq);
        this.sortColumns = Lists.newArrayList();
        this.sortOrders = Lists.newArrayList();
        Map<String, String> parameters = _localPrarameters.get();
        if (parameters != null) {
            String acceptPartialResult = parameters.get(PRM_ACCEPT_PARTIAL_RESULT);
            if (acceptPartialResult != null) {
                this.storageContext.setAcceptPartialResult(Boolean.parseBoolean(acceptPartialResult));
            }
            this.hasAdminPermission = HAS_ADMIN_PERMISSION.equals(parameters.get(PRM_PROJECT_PERMISSION));
            String acceptUserInfo = parameters.get(PRM_USER_AUTHEN_INFO);
            if (null != acceptUserInfo)
                this.olapAuthen.parseUserInfo(acceptUserInfo);
        }
    }

    public final int id;
    public final StorageContext storageContext;

    // query info
    public OLAPSchema olapSchema = null;
    public OLAPTableScan firstTableScan = null; // to be fact table scan except "select * from lookupTable"
    @Setter
    @Getter
    private OLAPRel topNode = null; // the context's toppest node
    @Setter
    @Getter
    private RelNode parentOfTopNode = null; // record the JoinRel that cuts off its children into new context(s), in other case it should be null
    public Set<OLAPTableScan> allTableScans = new HashSet<>();
    public Set<OLAPJoinRel> allOlapJoins = new HashSet<>();
    public Set<MeasureDesc> involvedMeasure = new HashSet<>();
    public TupleInfo returnTupleInfo = null;
    public boolean afterAggregate = false;
    public boolean afterHavingClauseFilter = false;
    public boolean afterLimit = false;
    @Setter
    @Getter
    private int limit = Integer.MAX_VALUE;
    public boolean limitPrecedesAggr = false;
    boolean afterTopJoin = false;
    @Setter
    @Getter
    private boolean hasJoin = false;
    @Setter
    @Getter
    private boolean hasPreCalcJoin = false;
    @Setter
    @Getter
    private boolean hasAgg = false;
    public boolean hasWindow = false;

    // cube metadata
    public IRealization realization;
    public RealizationCheck realizationCheck = new RealizationCheck();
    boolean fixedModel;

    @Getter
    @Setter
    private boolean hasSelected = false;
    public Set<TblColRef> allColumns = new HashSet<>();
    @Setter
    @Getter
    private Set<TblColRef> groupByColumns = Sets.newLinkedHashSet();
    @Setter
    @Getter
    // collect inner columns in group keys
    // this filed is used by CC proposer only
    private Set<TableColRefWIthRel> innerGroupByColumns = Sets.newLinkedHashSet();
    @Setter
    @Getter
    // collect inner columns in filter
    // this filed is used by CC proposer only
    private Set<TblColRef> innerFilterColumns = Sets.newLinkedHashSet();
    @Setter
    @Getter
    private Set<TblColRef> subqueryJoinParticipants = new HashSet<TblColRef>();//subqueryJoinParticipants will be added to groupByColumns(only when other group by co-exists) and allColumns
    public Set<TblColRef> metricsColumns = new HashSet<>();

    public List<FunctionDesc> aggregations = new ArrayList<>(); // storage level measure type, on top of which various sql aggr function may apply
    @Setter
    @Getter
    private List<FunctionDesc> constantAggregations = new ArrayList<>(); // agg like min(2),max(2),avg(2), not including count(1)
    public Set<TblColRef> filterColumns = new LinkedHashSet<>();
    public TupleFilter filter;
    public TupleFilter havingFilter;
    public List<JoinDesc> joins = new LinkedList<>();
    @Getter
    @Setter
    private JoinsGraph joinsGraph;
    @Getter
    @Setter
    private List<TblColRef> sortColumns;
    List<SQLDigest.OrderEnum> sortOrders;
    @Setter
    @Getter
    private Set<String> containedNotSupportedFunc = Sets.newHashSet();

    // rewrite info
    public Map<String, RelDataType> rewriteFields = new HashMap<>();

    // hive query
    public String sql = "";

    public OLAPAuthentication olapAuthen = new OLAPAuthentication();

    @Getter
    @Setter
    private boolean hasAdminPermission = false;

    public boolean isSimpleQuery() {
        return (joins.isEmpty()) && (groupByColumns.isEmpty()) && (aggregations.isEmpty());
    }

    public boolean isConstantQuery() {
        return allColumns.isEmpty() && aggregations.isEmpty();
    }

    public boolean isConstantQueryWithAggregations() {
        // deal with probing query like select min(2+2), max(2) from Table
        return allColumns.isEmpty() && aggregations.isEmpty() && !constantAggregations.isEmpty();
    }

    public boolean isConstantQueryWithoutAggregation() {
        // deal with probing query like select 1,current_date from Table
        return allColumns.isEmpty() && aggregations.isEmpty() && constantAggregations.isEmpty();
    }

    SQLDigest sqlDigest;

    public SQLDigest getSQLDigest() {
        if (sqlDigest == null) {
            sqlDigest = new SQLDigest(firstTableScan.getTableName(), Sets.newHashSet(allColumns),
                    Lists.newLinkedList(joins), // model
                    Lists.newArrayList(groupByColumns), Sets.newHashSet(subqueryJoinParticipants), // group by
                    Sets.newHashSet(metricsColumns), Lists.newArrayList(aggregations), // aggregation
                    Sets.newLinkedHashSet(filterColumns), filter, havingFilter, // filter
                    Lists.newArrayList(sortColumns), Lists.newArrayList(sortOrders), limit, limitPrecedesAggr, // sort & limit
                    Sets.newHashSet(involvedMeasure));
        }
        return sqlDigest;
    }

    public String getFirstTableIdentity() {
        return firstTableScan.getTableRef().getTableIdentity();
    }

    public boolean isFirstTableLookupTableInModel(NDataModel model) {
        return joins.isEmpty() && model.isLookupTable(getFirstTableIdentity());
    }

    public boolean hasPrecalculatedFields() {
        NLayoutCandidate candidate = storageContext.getCandidate();
        if (candidate == null) {
            return false;
        }
        boolean isTableIndex = candidate.getCuboidLayout().getIndex().isTableIndex();
        boolean isLookupTable = isFirstTableLookupTableInModel(realization.getModel());
        return !isTableIndex && !isLookupTable;
    }

    public void resetSQLDigest() {
        this.sqlDigest = null;
    }

    public boolean belongToContextTables(TblColRef tblColRef) {
        for (OLAPTableScan olapTableScan : this.allTableScans) {
            if (olapTableScan.getColumnRowType().getAllColumns().contains(tblColRef)) {
                return true;
            }
        }

        return false;
    }

    public boolean isOriginAndBelongToCtxTables(TblColRef tblColRef) {
        return belongToContextTables(tblColRef) && !tblColRef.getName().startsWith("_KY_");
    }

    public void setReturnTupleInfo(RelDataType rowType, ColumnRowType columnRowType) {
        TupleInfo info = new TupleInfo();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for (int i = 0; i < fieldList.size(); i++) {
            RelDataTypeField field = fieldList.get(i);
            TblColRef col = columnRowType == null ? null : columnRowType.getColumnByIndex(i);
            info.setField(field.getName(), col, i);
        }
        this.returnTupleInfo = info;
    }

    public void addSort(TblColRef col, SQLDigest.OrderEnum order) {
        if (col != null) {
            sortColumns.add(col);
            sortOrders.add(order);
        }
    }

    public void fixModel(NDataModel model, Map<String, String> aliasMap) {
        if (fixedModel)
            return;

        for (OLAPTableScan tableScan : this.allTableScans) {
            tableScan.fixColumnRowTypeWithModel(model, aliasMap);
        }
        fixedModel = true;
    }

    public void unfixModel() {
        if (!fixedModel)
            return;

        for (OLAPTableScan tableScan : this.allTableScans) {
            tableScan.unfixColumnRowTypeWithModel();
        }
        fixedModel = false;
    }

    public void clearCtxInfo() {
        //query info
        this.afterAggregate = false;
        this.afterHavingClauseFilter = false;
        this.afterLimit = false;
        this.limitPrecedesAggr = false;
        this.afterTopJoin = false;
        this.hasJoin = false;
        this.hasPreCalcJoin = false;
        this.hasAgg = false;
        this.hasWindow = false;

        this.allColumns.clear();
        this.groupByColumns.clear();
        this.subqueryJoinParticipants.clear();
        this.metricsColumns.clear();
        this.involvedMeasure.clear();
        this.allOlapJoins.clear();
        this.joins.clear();
        this.allTableScans.clear();
        this.filter = null;
        this.havingFilter = null;
        this.filterColumns.clear();

        this.aggregations.clear();

        this.sortColumns.clear();
        this.sortOrders.clear();

        this.joinsGraph = null;

        this.sqlDigest = null;
        this.getConstantAggregations().clear();
    }

    public void bindVariable(DataContext dataContext) {
        bindVariable(this.filter, dataContext);
    }

    private void bindVariable(TupleFilter filter, DataContext dataContext) {
        if (filter == null) {
            return;
        }

        for (TupleFilter childFilter : filter.getChildren()) {
            bindVariable(childFilter, dataContext);
        }

        if (filter instanceof CompareTupleFilter && dataContext != null) {
            CompareTupleFilter compFilter = (CompareTupleFilter) filter;
            for (Map.Entry<String, Object> entry : compFilter.getVariables().entrySet()) {
                String variable = entry.getKey();
                Object value = dataContext.get(variable);
                if (value != null) {
                    String str = value.toString();
                    if (compFilter.getColumn().getType().isDateTimeFamily())
                        str = String.valueOf(DateFormat.stringToMillis(str));

                    compFilter.bindVariable(variable, str);
                }

            }
        }
    }
    // ============================================================================

    public interface IAccessController {
        void check(List<OLAPContext> contexts, OLAPRel tree, KylinConfig config);
    }

    public void addInnerGroupColumns(KapRel rel, Collection<TblColRef> innerGroupColumns) {
        Set<TblColRef> innerGroupColumnsSet = new HashSet<>(innerGroupColumns);
        for (TblColRef tblColRef : innerGroupColumnsSet) {
            this.innerGroupByColumns.add(new TableColRefWIthRel(rel, tblColRef));
        }
    }

    public boolean isAnsweredByTableIndex() {
        if (this.storageContext.getCandidate() != null
                && this.storageContext.getCandidate().getCuboidLayout().getIndex().isTableIndex()) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "OLAPContext{" + "firstTableScan=" + firstTableScan + ", allTableScans=" + allTableScans
                + ", allOlapJoins=" + allOlapJoins + ", groupByColumns=" + groupByColumns + ", innerGroupByColumns="
                + innerGroupByColumns + ", innerFilterColumns=" + innerFilterColumns + ", aggregations=" + aggregations
                + ", filterColumns=" + filterColumns + '}';
    }
}
