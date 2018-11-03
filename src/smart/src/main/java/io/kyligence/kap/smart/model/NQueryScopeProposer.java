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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.RealizationChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.util.CubeUtils;

/**
 * Define Dimensions and Measures from SQLs
 */
public class NQueryScopeProposer extends NAbstractModelProposer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NQueryScopeProposer.class);

    public NQueryScopeProposer(NSmartContext.NModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void doPropose(NDataModel nDataModel) {
        LOGGER.trace("Propose scope for model [{}]", nDataModel.getId());
        ScopeBuilder scopeBuilder = new ScopeBuilder(nDataModel);

        ModelTree modelTree = modelContext.getModelTree();
        // Load from context
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            // fix models to update alias
            Map<String, String> matchingAlias = RealizationChooser.matches(nDataModel, ctx);
            ctx.fixModel(nDataModel, matchingAlias);
            scopeBuilder.setOLAPContext(ctx);
            ctx.unfixModel();
        }

        scopeBuilder.build();
    }

    private class ScopeBuilder {

        // column_identity <====> NamedColumn
        Map<String, NDataModel.NamedColumn> namedColsCandidate = Maps.newHashMap();
        Map<FunctionDesc, NDataModel.Measure> measureCandidate = Maps.newHashMap();

        int maxColId = -1;
        int maxMeasureId = NDataModel.MEASURE_ID_BASE - 1;

        NDataModel nDataModel;

        public ScopeBuilder(NDataModel nDataModel) {
            this.nDataModel = nDataModel;
            // Load from old model
            setNamedColumns(nDataModel.getAllNamedColumns());
            setMeasures(nDataModel.getAllMeasures());
            setJoins(nDataModel.getJoinTables().toArray(new JoinTableDesc[0]));
            // Add partition column to named columns
            setPartitionColumn(nDataModel.getPartitionDesc());
        }

        private ScopeBuilder setNamedColumns(List<NDataModel.NamedColumn> namedColumns) {
            for (NDataModel.NamedColumn namedColumn : namedColumns) {
                namedColsCandidate.put(namedColumn.aliasDotColumn, namedColumn);
                maxColId = Math.max(maxColId, namedColumn.id);
            }
            return this;
        }

        private ScopeBuilder setMeasures(List<Measure> measures) {
            for (NDataModel.Measure measure : measures) {
                measureCandidate.put(measure.getFunction(), measure);
                maxMeasureId = Math.max(maxMeasureId, measure.id);
            }
            return this;
        }

        private ScopeBuilder setPartitionColumn(PartitionDesc partitionDesc) {
            if (partitionDesc != null && partitionDesc.getPartitionDateColumn() != null) {
                String partitionColName = partitionDesc.getPartitionDateColumn();
                if (!namedColsCandidate.containsKey(partitionColName)) {
                    int newId = ++maxColId;
                    NDataModel.NamedColumn col = new NDataModel.NamedColumn();
                    col.name = partitionColName;
                    col.aliasDotColumn = partitionColName;
                    col.id = newId;
                    namedColsCandidate.put(partitionColName, col);
                }
            }
            return this;
        }

        JoinTableDesc[] joins = new JoinTableDesc[0];

        private ScopeBuilder setJoins(JoinTableDesc[] joins) {
            this.joins = joins;
            return this;
        }

        private ScopeBuilder setOLAPContext(OLAPContext ctx) {
            collectCtxColumns(ctx);
            collectCtxMeasures(ctx);
            return this;
        }

        Set<TblColRef> allTableColumns = Sets.newHashSet();

        private ScopeBuilder collectCtxColumns(OLAPContext ctx) {
            Set<TblColRef> tableColumns = Sets.newHashSet();
            for (OLAPTableScan tableScan : ctx.allTableScans) {
                tableColumns.addAll(tableScan.getTableRef().getColumns());
            }
            allTableColumns.addAll(tableColumns);

            TblColRef[] colArray = ctx.allColumns.toArray(new TblColRef[0]);
            Arrays.sort(colArray, Comparator.comparing(TblColRef::getIdentity));
            Set<TblColRef> allColumns = Sets.newLinkedHashSet(Arrays.asList(colArray));

            if (allColumns == null || allColumns.isEmpty()) {
                allColumns = tableColumns;
            }
            if (ctx.subqueryJoinParticipants != null)
                allColumns.addAll(ctx.subqueryJoinParticipants);

            for (JoinTableDesc join : joins) {
                TblColRef[] fks = join.getJoin().getForeignKeyColumns();
                allColumns.addAll(Arrays.asList(fks));
            }

            for (TblColRef tblColRef : allColumns) {
                boolean isDimension = (!ctx.getSQLDigest().isRawQuery)
                        && (ctx.filterColumns.contains(tblColRef) || ctx.groupByColumns.contains(tblColRef));
                if (namedColsCandidate.containsKey(tblColRef.getIdentity())) {
                    NamedColumn namedColumn = namedColsCandidate.get(tblColRef.getIdentity());
                    isDimension = namedColumn.isDimension() || isDimension;
                    namedColumn.status = isDimension ? NDataModel.ColumnStatus.DIMENSION
                            : NDataModel.ColumnStatus.EXIST;
                    continue;
                }
                int newId = ++maxColId;
                NDataModel.NamedColumn col = new NDataModel.NamedColumn();
                col.name = tblColRef.getName();
                col.aliasDotColumn = tblColRef.getIdentity();
                col.id = newId;
                col.status = isDimension ? NDataModel.ColumnStatus.DIMENSION : NDataModel.ColumnStatus.EXIST;
                namedColsCandidate.put(tblColRef.getIdentity(), col);
            }
            return this;
        }

        private ScopeBuilder collectCtxMeasures(OLAPContext ctx) {
            List<FunctionDesc> aggregations = Lists.newLinkedList(ctx.aggregations);
            for (FunctionDesc agg : aggregations) {
                if (measureCandidate.containsKey(agg)) {
                    measureCandidate.get(agg).tomb = false;
                    continue;
                }
                for (TblColRef tblColRef : agg.getParameter().getColRefs()) {
                    if (namedColsCandidate.containsKey(tblColRef.getIdentity())) {
                        NamedColumn namedCol = namedColsCandidate.get(tblColRef.getIdentity());
                        if (!namedCol.isExist()) {
                            namedCol.status = NDataModel.ColumnStatus.EXIST;
                        }
                    }
                }
                if (checkFunctionDesc(agg)) {
                    FunctionDesc newFunc = copyFunctionDesc(agg);
                    String name = String.format("%s_%s", newFunc.getExpression(),
                            newFunc.getParameter().getColRef().getName());
                    NDataModel.Measure measure = CubeUtils.newMeasure(newFunc, name, ++maxMeasureId);
                    measureCandidate.put(newFunc, measure);
                }
            }
            return this;
        }

        private void build() {
            Map<String, NDataModel.NamedColumn> dimensionCandidate = Maps.newHashMap();

            dimensionCandidate.putAll(namedColsCandidate);
            for (FunctionDesc agg : measureCandidate.keySet()) {
                if (!checkFunctionDesc(agg) || agg.getParameter() == null || agg.getParameter().getColRef() == null) {
                    continue;
                }
                String measureColName = agg.getParameter().getColRef().getIdentity();
                dimensionCandidate.remove(measureColName);
            }
            // FIXME work around empty dimension case
            // all named columns are used as measures
            if (dimensionCandidate.isEmpty()) {
                // dim place holder for none
                for (TblColRef candidate : allTableColumns) {
                    if (!namedColsCandidate.containsKey(candidate.getIdentity())) {
                        NDataModel.NamedColumn newNamedCol = new NDataModel.NamedColumn();
                        newNamedCol.name = candidate.getIdentity();
                        newNamedCol.aliasDotColumn = candidate.getIdentity();
                        newNamedCol.id = ++maxColId;
                        dimensionCandidate.put(candidate.getIdentity(), newNamedCol);
                        break;
                    }
                }
                namedColsCandidate.putAll(dimensionCandidate);
            }
            if (dimensionCandidate.isEmpty()) {
                throw new IllegalStateException("Suggest no dimension");
            }

            FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(nDataModel);
            if (!measureCandidate.containsKey(countStar)) {
                final Measure newMeasure = CubeUtils.newMeasure(countStar, "COUNT_ALL", ++maxMeasureId);
                measureCandidate.put(countStar, newMeasure);
            }

            List<NDataModel.NamedColumn> namedColumns = Lists.newArrayList(namedColsCandidate.values());
            namedColumns.sort(Comparator.comparingInt(NDataModel.NamedColumn::getId));
            nDataModel.setAllNamedColumns(namedColumns);

            List<NDataModel.Measure> measures = Lists.newArrayList(measureCandidate.values());
            measures.sort(Comparator.comparingInt(NDataModel.Measure::getId));
            nDataModel.setAllMeasures(measures);
        }

        private boolean checkFunctionDesc(FunctionDesc functionDesc) {
            List<TblColRef> colRefs = functionDesc.getParameter().getColRefs();
            if (colRefs == null || colRefs.isEmpty())
                return true;

            boolean isMaxMin = functionDesc.isMax() || functionDesc.isMin();
            for (TblColRef colRef : colRefs) {
                if (!colRef.isQualified()) {
                    return false;
                }

                if (isMaxMin && colRef.getType().isStringFamily()) {
                    return false;
                }

                if (isMaxMin && colRef.getType().isDateTimeFamily()) {
                    return false;
                }
            }

            return !functionDesc.isGrouping();
        }

        private ParameterDesc copyParameterDesc(ParameterDesc param) {
            ParameterDesc newParam = new ParameterDesc();
            newParam.setType(param.getType());
            if (param.isColumnType()) {
                newParam.setValue(param.getColRef().getIdentity());
            } else {
                newParam.setValue(param.getValue());
            }

            if (param.getNextParameter() != null)
                newParam.setNextParameter(copyParameterDesc(param.getNextParameter()));
            return newParam;
        }

        private FunctionDesc copyFunctionDesc(FunctionDesc orig) {
            TblColRef paramColRef = orig.getParameter().getColRef();
            ParameterDesc newParam = copyParameterDesc(orig.getParameter());
            return CubeUtils.newFunctionDesc(nDataModel, orig.getExpression(), newParam,
                    paramColRef == null ? null : paramColRef.getDatatype());
        }
    }
}
