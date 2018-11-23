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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.ColumnStatus;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.val;

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
        Map<String, NDataModel.NamedColumn> candidateNamedColumns = Maps.newLinkedHashMap();
        Map<FunctionDesc, NDataModel.Measure> candidateMeasures = Maps.newLinkedHashMap();
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

            // add all table columns of the ctx to allColumns,
            // use TreeSet can get a steady test result in different circumstances
            Set<TblColRef> allColumns = new TreeSet<>(Comparator.comparing(TblColRef::getIdentity));
            allColumns.addAll(allTableColumns);

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

            ctx.aggregations.forEach(agg -> {
                if (!candidateMeasures.containsKey(agg)) {

                    FunctionDesc fun = copyFunctionDesc(agg);
                    String name = String.format("%s_%s", fun.getExpression(), fun.getParameter().getColRef().getName());
                    NDataModel.Measure measure = CubeUtils.newMeasure(fun, name, ++maxMeasureId);
                    if (CubeUtils.isValidMeasure(agg)) {
                        candidateMeasures.put(fun, measure);
                    } else {
                        dimensionAsMeasureColumns.addAll(fun.getParameter().getColRefs());
                    }
                } else if (candidateMeasures.get(agg).tomb) {
                    String name = String.format("%s_%s", agg.getExpression(), agg.getParameter().getColRef().getName());
                    Measure measure = CubeUtils.newMeasure(agg, name, ++maxMeasureId);
                    candidateMeasures.put(agg, measure);
                }
            });
        }

        private void build() {

            // 1. ensure a dimension exist, otherwise throw exception
            Map<String, NDataModel.NamedColumn> candidateDimensions = Maps.newHashMap();
            for (val entry : candidateNamedColumns.entrySet()) {
                if (entry.getValue().isDimension()) {
                    candidateDimensions.put(entry.getKey(), entry.getValue());
                    break;
                }
            }

            // FIXME work around empty dimension case
            if (candidateDimensions.isEmpty()) {
                final TblColRef tblColRef = allTableColumns.iterator().next();
                NamedColumn column = transferToNamedColumn(tblColRef, ColumnStatus.DIMENSION);
                candidateDimensions.put(tblColRef.getIdentity(), column);
                candidateNamedColumns.put(tblColRef.getIdentity(), column);
            }

            Preconditions.checkState(!candidateDimensions.isEmpty(),
                    "Auto-modeling cannot suggest any available dimension.");

            // 2. publish all measures
            List<Measure> measures = Lists.newArrayList(candidateMeasures.values());
            Preconditions.checkState(
                    Ordering.natural().isOrdered(measures.stream().map(Measure::getId).collect(Collectors.toList())),
                    "Unsorted measures exception in process of proposing model.");
            dataModel.setAllMeasures(measures);

            // 3. publish all named columns
            List<NamedColumn> namedColumns = Lists.newArrayList(candidateNamedColumns.values());
            Preconditions.checkState(
                    Ordering.natural().isOrdered(measures.stream().map(Measure::getId).collect(Collectors.toList())),
                    "Unsorted named columns exception in process of proposing model.");
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
