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
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.ColumnStatus;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.util.CubeUtils;

/**
 * Define Dimensions and Measures from SQLs
 */
public class NQueryScopeProposer extends NAbstractModelProposer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NQueryScopeProposer.class);

    NQueryScopeProposer(NSmartContext.NModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void doPropose(NDataModel dataModel) {
        LOGGER.trace("Propose scope for model [{}]", dataModel.getId());
        ScopeBuilder scopeBuilder = new ScopeBuilder(dataModel);

        ModelTree modelTree = modelContext.getModelTree();
        // Load from context
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            Map<String, String> matchingAlias = RealizationChooser.matches(dataModel, ctx);
            ctx.fixModel(dataModel, matchingAlias);

            scopeBuilder.injectCandidateMeasure(ctx);
            scopeBuilder.injectAllTableColumns(ctx);
            scopeBuilder.injectCandidateColumns(ctx);

            ctx.unfixModel();
        }

        scopeBuilder.build();
    }

    private class ScopeBuilder {

        // column_identity <====> NamedColumn
        Map<String, NDataModel.NamedColumn> candidateNamedColumns = Maps.newHashMap();
        Map<FunctionDesc, NDataModel.Measure> candidateMeasures = Maps.newHashMap();
        Set<TblColRef> dimensionAsMeasureColumns = Sets.newHashSet();
        Set<TblColRef> allTableColumns = Sets.newHashSet();
        JoinTableDesc[] joins = new JoinTableDesc[0];

        private int maxColId = -1;
        private int maxMeasureId = NDataModel.MEASURE_ID_BASE - 1;

        private NDataModel dataModel;

        private ScopeBuilder(NDataModel dataModel) {
            this.dataModel = dataModel;

            // Inherit from old model
            inheritCandidateNamedColumns(dataModel);
            inheritCandidateMeasures(dataModel);
            inheritJoinTables(dataModel);
        }

        private void inheritCandidateNamedColumns(NDataModel dataModel) {

            List<NDataModel.NamedColumn> namedColumns = dataModel.getAllNamedColumns();
            namedColumns.forEach(column -> {
                candidateNamedColumns.put(column.getAliasDotColumn(), column);
                maxColId = Math.max(maxColId, column.getId());
            });

            // Add partition column to named columns
            PartitionDesc partitionDesc = dataModel.getPartitionDesc();
            if (partitionDesc != null && partitionDesc.getPartitionDateColumn() != null) {
                String partitionColName = partitionDesc.getPartitionDateColumn();
                if (!candidateNamedColumns.containsKey(partitionColName)) {
                    NDataModel.NamedColumn col = new NDataModel.NamedColumn();
                    col.setName(partitionColName);
                    col.setAliasDotColumn(partitionColName);
                    col.setId(++maxColId);
                    candidateNamedColumns.put(partitionColName, col);
                }
            }
        }

        private void inheritCandidateMeasures(NDataModel dataModel) {
            List<Measure> measures = dataModel.getAllMeasures();
            for (NDataModel.Measure measure : measures) {
                maxMeasureId = Math.max(maxMeasureId, measure.id);
                if (measure.tomb) {
                    continue;
                }
                candidateMeasures.put(measure.getFunction(), measure);
            }
        }

        private void inheritJoinTables(NDataModel dataModel) {
            this.joins = dataModel.getJoinTables().toArray(new JoinTableDesc[0]);
        }

        private void injectAllTableColumns(OLAPContext ctx) {
            ctx.allTableScans.forEach(tableScan -> allTableColumns.addAll(tableScan.getTableRef().getColumns()));
        }

        private void injectCandidateColumns(OLAPContext ctx) {

            // add all columns of ctx to all columns
            Set<TblColRef> allColumns = new TreeSet<>(Comparator.comparing(TblColRef::getIdentity));
            allColumns.addAll(ctx.allColumns);

            // add sub query join participants to all columns
            if (ctx.subqueryJoinParticipants != null) {
                allColumns.addAll(ctx.subqueryJoinParticipants);
            }

            // add all foreign key columns to all columns
            for (JoinTableDesc join : joins) {
                TblColRef[] fks = join.getJoin().getForeignKeyColumns();
                allColumns.addAll(Arrays.asList(fks));
            }

            // set status for all columns and put them into candidate named columns
            allColumns.forEach(tblColRef -> {
                ColumnStatus status;
                boolean isDimension = canTblColRefTreatAsDimension(ctx, tblColRef);
                if (candidateNamedColumns.containsKey(tblColRef.getIdentity())) {
                    NamedColumn namedColumn = candidateNamedColumns.get(tblColRef.getIdentity());
                    isDimension = namedColumn.isDimension() || isDimension;
                    status = isDimension ? ColumnStatus.DIMENSION : ColumnStatus.EXIST;
                    namedColumn.setStatus(status);
                } else {
                    status = isDimension ? ColumnStatus.DIMENSION : ColumnStatus.EXIST;
                    final NamedColumn column = transferToNamedColumn(tblColRef, status);
                    candidateNamedColumns.put(tblColRef.getIdentity(), column);
                }
            });
        }

        private void injectCandidateMeasure(OLAPContext ctx) {
            List<FunctionDesc> aggregations = Lists.newLinkedList(ctx.aggregations);
            for (FunctionDesc agg : aggregations) {
                if (candidateMeasures.containsKey(agg)) {
                    candidateMeasures.get(agg).tomb = false;//TODO different manual measure may share the same agg
                    continue;
                }

                agg.getParameter().getColRefs().stream()
                        .filter(tblColRef -> candidateNamedColumns.containsKey(tblColRef.getIdentity()))
                        .map(tblColRef -> candidateNamedColumns.get(tblColRef.getIdentity()))
                        .filter(namedCol -> !namedCol.isExist())
                        .forEach(namedCol -> namedCol.setStatus(ColumnStatus.EXIST));

                FunctionDesc newFunc = copyFunctionDesc(agg);
                String name = String.format("%s_%s", newFunc.getExpression(),
                        newFunc.getParameter().getColRef().getName());
                NDataModel.Measure measure = CubeUtils.newMeasure(newFunc, name, ++maxMeasureId);

                if (CubeUtils.isValidMeasure(agg)) {
                    candidateMeasures.put(newFunc, measure);
                } else {
                    dimensionAsMeasureColumns.addAll(newFunc.getParameter().getColRefs());
                }
            }
        }

        private void build() {

            // 1. publish all dimensions
            Map<String, NDataModel.NamedColumn> candidateDimensions = Maps.newHashMap(candidateNamedColumns);
            candidateMeasures.keySet().stream() //
                    .map(functionDesc -> functionDesc.getParameter().getColRefs()) //
                    .flatMap(List::stream) //
                    .filter(Objects::nonNull).map(TblColRef::getIdentity) //
                    .filter(candidateDimensions::containsKey) //
                    .filter(col -> !candidateDimensions.get(col).isDimension()) //
                    .forEach(candidateDimensions::remove);

            // FIXME work around empty dimension case
            // all named columns are used as measures
            if (candidateDimensions.isEmpty()) {
                // dim place holder for none
                for (TblColRef candidate : allTableColumns) {
                    if (!candidateNamedColumns.containsKey(candidate.getIdentity())) {
                        NDataModel.NamedColumn newNamedCol = new NDataModel.NamedColumn();
                        newNamedCol.setName(candidate.getIdentity());
                        newNamedCol.setAliasDotColumn(candidate.getIdentity());
                        newNamedCol.setId(++maxColId);
                        candidateDimensions.put(candidate.getIdentity(), newNamedCol);
                        break;
                    }
                }
                candidateNamedColumns.putAll(candidateDimensions);
            }
            if (candidateDimensions.isEmpty()) {
                throw new IllegalStateException("Suggest no dimension");
            }
            candidateDimensions.forEach((colName, col) -> col.setStatus(NDataModel.ColumnStatus.DIMENSION));

            // 2. publish all measures
            FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(dataModel);
            if (!candidateMeasures.containsKey(countStar)) {
                final Measure newMeasure = CubeUtils.newMeasure(countStar, "COUNT_ALL", ++maxMeasureId);
                candidateMeasures.put(countStar, newMeasure);
            }

            List<NDataModel.Measure> measures = Lists.newArrayList(candidateMeasures.values());
            measures.sort(Comparator.comparingInt(NDataModel.Measure::getId));
            dataModel.setAllMeasures(measures);

            // 3. publish all named columns
            List<NDataModel.NamedColumn> namedColumns = Lists.newArrayList(candidateNamedColumns.values());
            namedColumns.sort(Comparator.comparingInt(NDataModel.NamedColumn::getId));
            dataModel.setAllNamedColumns(namedColumns);
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
            return CubeUtils.newFunctionDesc(dataModel, orig.getExpression(), newParam,
                    paramColRef == null ? null : paramColRef.getDatatype());
        }

        private boolean canTblColRefTreatAsDimension(OLAPContext ctx, TblColRef tblColRef) {
            return !ctx.getSQLDigest().isRawQuery && (ctx.filterColumns.contains(tblColRef)
                    || ctx.groupByColumns.contains(tblColRef) || ctx.subqueryJoinParticipants.contains(tblColRef)
                    || dimensionAsMeasureColumns.contains(tblColRef));
        }

        private NamedColumn transferToNamedColumn(TblColRef colRef, ColumnStatus status) {
            NamedColumn col = new NamedColumn();
            col.setName(colRef.getName());
            col.setAliasDotColumn(colRef.getIdentity());
            col.setId(++maxColId);
            col.setStatus(status);
            return col;
        }
    }
}
