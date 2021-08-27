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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.ColumnStatus;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import io.kyligence.kap.metadata.recommendation.util.RawRecUtil;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Define Dimensions and Measures from SQLs
 */
@Slf4j
public class QueryScopeProposer extends AbstractModelProposer {

    QueryScopeProposer(AbstractContext.ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    protected void execute(NDataModel dataModel) {
        log.trace("Propose scope for model [{}]", dataModel.getId());
        ScopeBuilder scopeBuilder = new ScopeBuilder(dataModel, getModelContext());

        ModelTree modelTree = modelContext.getModelTree();
        // Load from context
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            if (!isValidOlapContext(ctx)) {
                continue;
            }

            // When injecting the columns and measures of OLAPContext into the scopeBuilder fails,
            // discard this OLAPContext and set blocking cause to the related SQL.
            try {
                Map<String, String> matchingAlias = RealizationChooser.matchJoins(dataModel, ctx);
                ctx.fixModel(dataModel, matchingAlias);
                scopeBuilder.resetSkipSuggestDimensions();
                scopeBuilder.injectCandidateMeasure(ctx);
                scopeBuilder.injectAllTableColumns(ctx);
                scopeBuilder.injectCandidateColumns(ctx);
            } catch (Exception e) {
                val accelerateInfoMap = modelContext.getProposeContext().getAccelerateInfoMap();
                AccelerateInfo accelerateInfo = accelerateInfoMap.get(ctx.sql);
                Preconditions.checkNotNull(accelerateInfo);
                accelerateInfo.setFailedCause(e);
            } finally {
                ctx.unfixModel();
            }
        }

        scopeBuilder.build();
    }

    protected static class ScopeBuilder {

        // column_identity <====> NamedColumn
        Map<String, NDataModel.NamedColumn> candidateNamedColumns = Maps.newLinkedHashMap();
        Map<FunctionDesc, NDataModel.Measure> candidateMeasures = Maps.newLinkedHashMap();
        Set<TblColRef> dimensionAsMeasureColumns = Sets.newHashSet();
        private final Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();

        Set<TblColRef> allTableColumns = Sets.newHashSet();
        JoinTableDesc[] joins = new JoinTableDesc[0];

        private int maxColId = -1;
        private int maxMeasureId = NDataModel.MEASURE_ID_BASE - 1;

        private final NDataModel dataModel;
        private final AbstractContext.ModelContext modelContext;
        private boolean skipSuggestDimensions;

        public void resetSkipSuggestDimensions() {
            skipSuggestDimensions = false;
        }

        protected ScopeBuilder(NDataModel dataModel, AbstractContext.ModelContext modelContext) {
            this.dataModel = dataModel;
            this.modelContext = modelContext;

            // Inherit from old model
            inheritCandidateNamedColumns(dataModel);
            inheritCandidateMeasures(dataModel);
            inheritJoinTables(dataModel);

            /* ccMap used for recording all computed columns for generate uniqueFlag of RecItemV2 */
            dataModel.getComputedColumnDescs().forEach(cc -> {
                String aliasDotName = cc.getTableAlias() + "." + cc.getColumnName();
                ccMap.putIfAbsent(aliasDotName, cc);
            });
        }

        private void inheritCandidateNamedColumns(NDataModel dataModel) {
            List<NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
            for (NamedColumn column : allNamedColumns) {
                maxColId = Math.max(maxColId, column.getId());
                // Forward compatibility, ensure col name is unique
                if (!column.isExist()) {
                    continue;
                }
                column.setName(column.getName());
                candidateNamedColumns.put(column.getAliasDotColumn(), column);
            }
        }

        private void inheritCandidateMeasures(NDataModel dataModel) {
            List<Measure> measures = dataModel.getAllMeasures();
            for (NDataModel.Measure measure : measures) {
                maxMeasureId = Math.max(maxMeasureId, measure.getId());
                if (measure.isTomb()) {
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
            if (skipSuggestDimensions) {
                return;
            }

            // set status for all columns and put them into candidate named columns
            Map<String, TblColRef> fKAsDimensionMap = ctx.collectFKAsDimensionMap(modelContext.getChecker());
            allColumns.forEach(tblColRef -> {
                ColumnStatus status;
                boolean canTreatAsDim = canTblColRefTreatAsDimension(fKAsDimensionMap, tblColRef)
                        || canTblColRefTreatAsDimension(ctx, tblColRef);
                boolean isNewDimension = canTreatAsDim;
                if (candidateNamedColumns.containsKey(tblColRef.getIdentity())) {
                    NamedColumn namedColumn = candidateNamedColumns.get(tblColRef.getIdentity());
                    boolean existingDimension = namedColumn.isDimension();
                    status = existingDimension || canTreatAsDim ? ColumnStatus.DIMENSION : ColumnStatus.EXIST;
                    isNewDimension = !existingDimension && canTreatAsDim;
                    namedColumn.setStatus(status);
                } else {
                    status = canTreatAsDim ? ColumnStatus.DIMENSION : ColumnStatus.EXIST;
                    final NamedColumn column = transferToNamedColumn(tblColRef, status);
                    candidateNamedColumns.put(tblColRef.getIdentity(), column);
                }

                if (isNewDimension) {
                    addDimRecommendation(candidateNamedColumns.get(tblColRef.getIdentity()), tblColRef);
                }
            });
        }

        private void addDimRecommendation(NamedColumn column, TblColRef tblColRef) {
            if (!modelContext.getProposeContext().needCollectRecommendations()) {
                return;
            }

            String uniqueContent = RawRecUtil.dimensionUniqueContent(tblColRef, ccMap);
            if (modelContext.getUniqueContentToFlag().containsKey(uniqueContent)) {
                return;
            }

            DimensionRecItemV2 item = new DimensionRecItemV2();
            item.setColumn(column);
            item.setDataType(tblColRef.getDatatype());
            item.setCreateTime(System.currentTimeMillis());
            item.setUniqueContent(uniqueContent);
            item.setUuid(String.format(Locale.ROOT, "dimension_%s", RandomUtil.randomUUIDStr()));
            modelContext.getDimensionRecItemMap().putIfAbsent(item.getUuid(), item);
        }

        private void injectCandidateMeasure(OLAPContext ctx) {
            for (FunctionDesc agg : ctx.aggregations) {
                if (modelContext.getChecker().isMeasureOnLookupTable(agg)) {
                    skipSuggestDimensions = true;
                    log.debug("Unsupported measure on dimension table, {}", agg.toString());
                    return;
                }
            }
            ctx.aggregations.forEach(agg -> {
                Set<String> paramNames = Sets.newHashSet();
                agg.getParameters().forEach(parameterDesc -> {
                    paramNames.add(parameterDesc.getColRef().getIdentity().replaceAll("\\.", "_"));
                });
                boolean isNewMeasure = false;
                if (!candidateMeasures.containsKey(agg) && agg.isAdvanceDimAsMeasure()) {
                    dimensionAsMeasureColumns.addAll(agg.getSourceColRefs());
                } else if (!candidateMeasures.containsKey(agg)) {
                    FunctionDesc fun = copyFunctionDesc(agg);
                    String name = String.format(Locale.ROOT, "%s_%s", fun.getExpression(),
                            String.join("_", paramNames));
                    NDataModel.Measure measure = CubeUtils.newMeasure(fun, name, ++maxMeasureId);
                    if (CubeUtils.isValidMeasure(agg)) {
                        candidateMeasures.put(fun, measure);
                        isNewMeasure = true;
                    } else {
                        dimensionAsMeasureColumns.addAll(fun.getColRefs());
                    }
                } else if (candidateMeasures.get(agg).isTomb()) {
                    String name = String.format(Locale.ROOT, "%s_%s", agg.getExpression(),
                            String.join("_", paramNames));
                    Measure measure = CubeUtils.newMeasure(agg, name, ++maxMeasureId);
                    candidateMeasures.put(agg, measure);
                    isNewMeasure = true;
                }

                if (isNewMeasure) {
                    Measure measure = candidateMeasures.get(agg);
                    addMeasureRecommendation(measure);
                }
            });
        }

        private void addMeasureRecommendation(Measure measure) {
            if (!modelContext.getProposeContext().needCollectRecommendations()) {
                return;
            }

            String uniqueContent = RawRecUtil.measureUniqueContent(measure, ccMap);
            if (modelContext.getUniqueContentToFlag().containsKey(uniqueContent)) {
                return;
            }

            MeasureRecItemV2 item = new MeasureRecItemV2();
            item.setMeasure(measure);
            item.setCreateTime(System.currentTimeMillis());
            item.setUniqueContent(uniqueContent);
            item.setUuid(String.format(Locale.ROOT, "measure_%s", RandomUtil.randomUUIDStr()));
            modelContext.getMeasureRecItemMap().putIfAbsent(item.getUuid(), item);
        }

        private void build() {

            // 1. publish all measures
            List<Measure> measures = Lists.newArrayList(candidateMeasures.values());
            NDataModel.checkDuplicateMeasure(measures);
            dataModel.setAllMeasures(measures);

            // 2. publish all named columns
            List<NamedColumn> namedColumns = Lists.newArrayList(candidateNamedColumns.values());
            NDataModel.changeNameIfDup(namedColumns);

            NDataModel.checkDuplicateColumn(namedColumns);
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
            return newParam;
        }

        private FunctionDesc copyFunctionDesc(FunctionDesc orig) {
            TblColRef paramColRef = orig.getParameters().get(0).getColRef();
            List<ParameterDesc> newParams = Lists.newArrayList();
            orig.getParameters().forEach(parameterDesc -> {
                newParams.add(copyParameterDesc(parameterDesc));
            });
            return CubeUtils.newFunctionDesc(dataModel, orig.getExpression(), newParams,
                    paramColRef == null ? null : paramColRef.getDatatype());
        }

        private boolean canTblColRefTreatAsDimension(OLAPContext ctx, TblColRef tblColRef) {
            if (modelContext.getChecker().isColRefDependsLookupTable(tblColRef)) {
                return false;
            }
            if (ctx.getSQLDigest().isRawQuery) {
                return ctx.allColumns.contains(tblColRef);
            } else {
                return ctx.filterColumns.contains(tblColRef) || ctx.getGroupByColumns().contains(tblColRef)
                        || ctx.getSubqueryJoinParticipants().contains(tblColRef)
                        || dimensionAsMeasureColumns.contains(tblColRef);
            }
        }

        private boolean canTblColRefTreatAsDimension(Map<String, TblColRef> fKAsDimensionMap, TblColRef tblColRef) {
            return fKAsDimensionMap.containsKey(tblColRef.getCanonicalName());
        }

        protected NamedColumn transferToNamedColumn(TblColRef colRef, ColumnStatus status) {
            NamedColumn col = new NamedColumn();
            col.setName(colRef.getName());
            col.setAliasDotColumn(colRef.getIdentity());
            col.setId(++maxColId);
            col.setStatus(status);
            return col;
        }
    }
}
