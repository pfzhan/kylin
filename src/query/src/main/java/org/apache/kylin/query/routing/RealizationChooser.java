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

package org.apache.kylin.query.routing;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import lombok.var;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.cuboid.NLookupCandidate;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;


public class RealizationChooser {

    private static final String DATE = "date";
    private static final String TIMESTAMP = "timestamp";
    private static final String VARCHAR = "varchar";
    private static final String STRING = "string";
    private static final String INTEGER = "integer";
    private static final String BIGINT = "bigint";

    private static final Logger logger = LoggerFactory.getLogger(RealizationChooser.class);

    private RealizationChooser() {
    }

    // select models for given contexts, return realization candidates for each context
    public static void selectLayoutCandidate(List<OLAPContext> contexts) {
        // try different model for different context
        for (OLAPContext ctx : contexts) {
            if (ctx.isConstantQueryWithAggregations())
                continue;

            ctx.realizationCheck = new RealizationCheck();
            attemptSelectCandidate(ctx);
            Preconditions.checkNotNull(ctx.realization);
        }
    }

    @VisibleForTesting
    public static void attemptSelectCandidate(OLAPContext context) {
        context.setHasSelected(true);
        // Step 1. get model through matching fact table with query
        Multimap<NDataModel, IRealization> modelMap = makeOrderedModelMap(context);
        if (modelMap.size() == 0) {
            throw new NoRealizationFoundException("No model found for " + toErrorMsg(context));
        }
        logger.trace("Models matched fact table {}: {}", context.firstTableScan.getTableName(), modelMap.values());
        List<Candidate> candidates = Lists.newArrayList();
        Map<NDataModel, Map<String, String>> model2AliasMap = Maps.newHashMap();

        // Step 2.1 try to exactly match model
        logger.debug("Context join graph: {}", context.getJoinsGraph());
        for (NDataModel model : modelMap.keySet()) {
            OLAPContextProp preservedOLAPContext = QueryRouter.preservePropsBeforeRewrite(context);
            Candidate candidate = selectRealizationFromModel(model, context, false, modelMap, model2AliasMap);
            logger.info("context & model({}, {}) match info: {}", model.getUuid(), model.getAlias(), candidate != null);
            if (candidate != null) {
                candidates.add(candidate);
            }
            // discard the props of OLAPContext modified by rewriteCcInnerCol
            QueryRouter.restoreOLAPContextProps(context, preservedOLAPContext);
        }

        // Step 2.2 if no exactly model and user config to try partial model match, then try partial match model
        if (CollectionUtils.isEmpty(candidates)
                && KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel()) {
            for (NDataModel model : modelMap.keySet()) {
                OLAPContextProp preservedOLAPContext = QueryRouter.preservePropsBeforeRewrite(context);
                Candidate candidate = selectRealizationFromModel(model, context, true, modelMap, model2AliasMap);
                if (candidate != null) {
                    candidates.add(candidate);
                }
                // discard the props of OLAPContext modified by rewriteCcInnerCol
                QueryRouter.restoreOLAPContextProps(context, preservedOLAPContext);
            }
            context.storageContext.setPartialMatchModel(CollectionUtils.isNotEmpty(candidates));
        }

        // Step 3. find the lowest-cost candidate
        Collections.sort(candidates);
        logger.trace("Cost Sorted Realizations {}", candidates);
        if (!candidates.isEmpty()) {
            Candidate selectedCandidate = candidates.get(0);
            QueryRouter.restoreOLAPContextProps(context, selectedCandidate.getRewrittenCtx());
            context.fixModel(selectedCandidate.getRealization().getModel(),
                    model2AliasMap.get(selectedCandidate.getRealization().getModel()));
            adjustForCapabilityInfluence(selectedCandidate, context);

            context.realization = selectedCandidate.realization;
            if (selectedCandidate.capability.getSelectedCandidate() instanceof NLookupCandidate) {
                context.storageContext
                        .setUseSnapshot(context.isFirstTableLookupTableInModel(context.realization.getModel()));
            } else {
                Set<TblColRef> dimensions = Sets.newHashSet();
                Set<FunctionDesc> metrics = Sets.newHashSet();
                buildDimensionsAndMetrics(context.getSQLDigest(), dimensions, metrics, (NDataflow) context.realization);
                buildStorageContext(context.storageContext, dimensions, metrics,
                        (NLayoutCandidate) selectedCandidate.capability.getSelectedCandidate(), selectedCandidate.getPrunedSegments());
                fixContextForTableIndexAnswerNonRawQuery(context);
            }
            return;
        }

        throw new NoRealizationFoundException("No realization found for " + toErrorMsg(context));
    }

    private static Candidate selectRealizationFromModel(NDataModel model, OLAPContext context, boolean isPartialMatch,
                                                        Multimap<NDataModel, IRealization> modelMap, Map<NDataModel, Map<String, String>> model2AliasMap) {
        final Map<String, String> map = matchJoins(model, context, isPartialMatch);
        if (map == null) {
            return null;
        }
        context.fixModel(model, map);
        model2AliasMap.put(model, map);
        logger.trace("Model {} join matched", model);

        preprocessOlapCtx(context);
        // check ready segments
        if (!hasReadySegments(model)) {
            context.unfixModel();
            logger.info("Exclude this model {} because there are no ready segments", model.getAlias());
            return null;
        }
        Candidate candidate = QueryRouter.selectRealization(context, Sets.newHashSet(modelMap.get(model)),
                model2AliasMap.get(model), pruneSegments(model, context));
        if (candidate != null) {
            logger.trace("Model {} QueryRouter matched", model);
        } else {
            logger.trace("Model {} failed in QueryRouter matching", model);
        }
        context.unfixModel();
        return candidate;
    }

    private static boolean hasReadySegments(NDataModel model) {
        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject()).getDataflow(model.getUuid());
        return dataflow.hasReadySegments();
    }

    private static List<NDataSegment> pruneSegments(NDataModel model, OLAPContext olapContext) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val projectName = model.getProject();
        val dataflow = NDataflowManager.getInstance(kylinConfig, projectName).getDataflow(model.getUuid());
        val projectInstance = NProjectManager.getInstance(kylinConfig).getProject(projectName);
        if (!projectInstance.getConfig().isHeterogeneousSegmentEnabled()) {
            return Lists.newArrayList(dataflow.getLatestReadySegment());
        }
        val allReadySegments = dataflow.getQueryableSegments();
        val partitionDesc = dataflow.getModel().getPartitionDesc();
        // no partition column
        if (partitionDesc == null || partitionDesc.getPartitionDateColumn() == null) {
            logger.trace("No partition column");
            return allReadySegments;
        }

        val partitionColumn = partitionDesc.getPartitionDateColumnRef();
        val dateFormat = partitionDesc.getPartitionDateFormat();
        val filterColumns = olapContext.filterColumns;
        // sql filter columns do not include partition column
        if (!filterColumns.contains(partitionColumn)) {
            logger.trace("Filter columns do not contain partition column");
            return allReadySegments;
        }

        val selectedSegments = Lists.<NDataSegment>newArrayList();
        val filterConditions = olapContext.expandedFilterConditions;
        val relOptCluster = olapContext.firstTableScan.getCluster();
        val rexBuilder = relOptCluster.getRexBuilder();
        val rexSimplify = new RexSimplify(relOptCluster.getRexBuilder(),
                RelOptPredicateList.EMPTY, true, relOptCluster.getPlanner().getExecutor());
        val simplifiedSqlFilter = rexSimplify.simplifyAnds(filterConditions);
        // sql filter condition is always false
        if (simplifiedSqlFilter.isAlwaysFalse()) {
            logger.trace("SQL filter condition is always false, pruning all ready segments");
            return selectedSegments;
        }
        // sql filter condition is always true
        if (simplifiedSqlFilter.isAlwaysTrue()) {
            logger.trace("SQL filter condition is always true, pruning no segment");
            return allReadySegments;
        }
        val partitionColInputRef = getPartitionColInputRef(partitionColumn, olapContext.allTableScans);
        if (partitionColInputRef == null) {
            logger.debug("Cannot find partition column {} in context {} all tableScans", partitionColumn.getIdentity(), olapContext.id);
            return allReadySegments;
        }

        for (NDataSegment dataSegment : allReadySegments) {
            try {
                val segmentRanges = transformSegment2RexCall(dataSegment, dateFormat, rexBuilder, partitionColInputRef, partitionColumn.getType());
                // compare with segment start
                val segmentStartPredicate = RelOptPredicateList.of(rexBuilder, Lists.newArrayList(segmentRanges.getFirst()));
                var simplifiedWithPredicate = rexSimplify.withPredicates(segmentStartPredicate).simplify(simplifiedSqlFilter);
                if (simplifiedWithPredicate.isAlwaysFalse()) {
                    continue;
                }
                // compare with segment end
                val segmentEndPredicate = RelOptPredicateList.of(rexBuilder, Lists.newArrayList(segmentRanges.getSecond()));
                simplifiedWithPredicate = rexSimplify.withPredicates(segmentEndPredicate).simplify(simplifiedWithPredicate);
                if (!simplifiedWithPredicate.isAlwaysFalse()) {
                    selectedSegments.add(dataSegment);
                }
            } catch (Exception ex) {
                logger.warn("Segment pruning error: ", ex);
                selectedSegments.add(dataSegment);
            }
        }

        return selectedSegments;
    }

    private static Pair<RexNode, RexNode> transformSegment2RexCall(NDataSegment dataSegment, String dateFormat, RexBuilder rexBuilder, RexInputRef partitionColInputRef, DataType partitionColType) {
        String start;
        String end;
        if (dataSegment.isOffsetCube()) {
            start = DateFormat.formatToDateStr(dataSegment.getKSRange().getStart(), dateFormat);
            end = DateFormat.formatToDateStr(dataSegment.getKSRange().getEnd(), dateFormat);
        } else {
            start = DateFormat.formatToDateStr(dataSegment.getTSRange().getStart(), dateFormat);
            end = DateFormat.formatToDateStr(dataSegment.getTSRange().getEnd(), dateFormat);
        }

        val startRexLiteral = transformSegmentRange2RexLiteral(rexBuilder, start, partitionColType);
        val endRexLiteral = transformSegmentRange2RexLiteral(rexBuilder, end, partitionColType);
        val greaterThanOrEqualCall = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, Lists.newArrayList(partitionColInputRef, startRexLiteral));
        val lessThanCall = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, Lists.newArrayList(partitionColInputRef, endRexLiteral));
        return Pair.newPair(greaterThanOrEqualCall, lessThanCall);
    }

    private static RexNode transformSegmentRange2RexLiteral(RexBuilder rexBuilder, String time, DataType partitionColType) {
        switch (partitionColType.getName()) {
            case DATE:
                return rexBuilder.makeDateLiteral(new DateString(time));
            case TIMESTAMP:
                var relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
                return rexBuilder.makeTimestampLiteral(new TimestampString(time), relDataType.getPrecision());
            case VARCHAR:
            case STRING:
                return rexBuilder.makeLiteral(time);
            case INTEGER:
                relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
                return rexBuilder.makeLiteral(Integer.valueOf(time), relDataType, false);
            case BIGINT:
                relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
                return rexBuilder.makeLiteral(Long.valueOf(time), relDataType, false);
            default:
                throw new IllegalArgumentException(String.format("%s data type is not supported for partition column", partitionColType));
        }
    }

    private static RexInputRef getPartitionColInputRef(TblColRef partitionCol, Set<OLAPTableScan> tableScans) {
        for (OLAPTableScan tableScan : tableScans) {
            val tableIdentity = tableScan.getTableName();
            if (tableIdentity.equals(partitionCol.getTable())) {
                val index = tableScan.getColumnRowType().getAllColumns().indexOf(partitionCol);
                if (index >= 0) {
                    return RexInputRef.of(tableIdentity + "." + partitionCol.getName(), index, tableScan.getRowType());
                }
                return null;
            }
        }

        return null;
    }

    public static void fixContextForTableIndexAnswerNonRawQuery(OLAPContext context) {
        if (KylinConfig.getInstanceFromEnv().isUseTableIndexAnswerNonRawQuery()
                && !context.storageContext.isEmptyLayout() && context.isAnsweredByTableIndex()) {
            if (!context.aggregations.isEmpty()) {
                List<FunctionDesc> aggregations = context.aggregations;
                HashSet<TblColRef> needDimensions = Sets.newHashSet();
                for (FunctionDesc aggregation : aggregations) {
                    List<ParameterDesc> parameters = aggregation.getParameters();
                    for (ParameterDesc aggParameter : parameters) {
                        needDimensions.addAll(aggParameter.getColRef().getSourceColumns());
                    }
                }
                context.storageContext.getDimensions().addAll(needDimensions);
                context.aggregations.clear();
            }
            if (context.getSQLDigest().aggregations != null) {
                context.getSQLDigest().aggregations.clear();
            }
            if (context.storageContext.getMetrics() != null) {
                context.storageContext.getMetrics().clear();
            }
        }
    }

    private static void adjustForCapabilityInfluence(Candidate chosen, OLAPContext olapContext) {
        CapabilityResult capability = chosen.getCapability();

        for (CapabilityResult.CapabilityInfluence inf : capability.influences) {

            if (inf instanceof CapabilityResult.DimensionAsMeasure) {
                FunctionDesc functionDesc = ((CapabilityResult.DimensionAsMeasure) inf).getMeasureFunction();
                functionDesc.setDimensionAsMetric(true);
                addToContextGroupBy(functionDesc.getColRefs(), olapContext);
                olapContext.resetSQLDigest();
                olapContext.getSQLDigest();
                logger.info("Adjust DimensionAsMeasure for {}", functionDesc);
            } else {

                MeasureDesc involvedMeasure = inf.getInvolvedMeasure();
                if (involvedMeasure == null)
                    continue;

                involvedMeasure.getFunction().getMeasureType().adjustSqlDigest(involvedMeasure,
                        olapContext.getSQLDigest());
            }
        }
    }

    private static void addToContextGroupBy(List<TblColRef> colRefs, OLAPContext context) {
        for (TblColRef col : colRefs) {
            if (!col.isInnerColumn() && context.belongToContextTables(col))
                context.getGroupByColumns().add(col);
        }
    }

    private static void preprocessOlapCtx(OLAPContext context) {
        if (CollectionUtils.isEmpty(context.aggregations))
            return;
        Iterator<FunctionDesc> it = context.aggregations.iterator();
        while (it.hasNext()) {
            FunctionDesc func = it.next();
            if (FunctionDesc.FUNC_GROUPING.equalsIgnoreCase(func.getExpression())) {
                it.remove();
            } else if (BitmapMeasureType.FUNC_INTERSECT_COUNT_DISTINCT.equalsIgnoreCase(func.getExpression())) {
                TblColRef col = func.getColRefs().get(1);
                context.getGroupByColumns().add(col);
            }
        }
    }

    private static void buildStorageContext(StorageContext context, Set<TblColRef> dimensions,
                                            Set<FunctionDesc> metrics, NLayoutCandidate selectedCandidate, List<NDataSegment> prunedSegments) {
        if (CollectionUtils.isEmpty(prunedSegments)) {
            context.setEmptyLayout(true);
            logger.info("for context {}, chose empty layout", context.getCtxId());
            return;
        }
        LayoutEntity cuboidLayout = selectedCandidate.getCuboidLayout();
        context.setCandidate(selectedCandidate);
        context.setDimensions(dimensions);
        context.setMetrics(metrics);
        context.setCuboidLayoutId(cuboidLayout.getId());
        context.setPrunedSegments(prunedSegments);
        val segmentIds = prunedSegments.stream().map(NDataSegment::getId).collect(Collectors.toList());
        logger.debug("for context {}, chosen model: {}, its join: {}, layout: {}, dimensions: {}, measures: {}, segments: {}",
                context.getCtxId(), cuboidLayout.getModel().getAlias(), cuboidLayout.getModel().getJoinsGraph(),
                cuboidLayout.getId(), cuboidLayout.getOrderedDimensions(), cuboidLayout.getOrderedMeasures(), segmentIds);

    }

    private static void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions,
                                                  Collection<FunctionDesc> metrics, NDataflow dataflow) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric() && !func.isGrouping()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision

                if (FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(func.getExpression())) {
                    dataflow.getMeasures().stream()
                            .filter(measureDesc -> measureDesc.getFunction().getReturnType().equals("bitmap") && func
                                    .getParameters().get(0).equals(measureDesc.getFunction().getParameters().get(0)))
                            .forEach(measureDesc -> metrics.add(measureDesc.getFunction()));
                    dimensions.add(func.getParameters().get(1).getColRef());
                } else {
                    FunctionDesc aggrFuncFromDataflowDesc = dataflow.findAggrFuncFromDataflowDesc(func);
                    metrics.add(aggrFuncFromDataflowDesc);
                }
            } else if (func.isDimensionAsMetric()) {
                FunctionDesc funcUsedDimenAsMetric = findAggrFuncFromDataflowDesc(func, dataflow);
                dimensions.addAll(funcUsedDimenAsMetric.getColRefs());

                Set<TblColRef> groupbyCols = Sets.newLinkedHashSet(sqlDigest.groupbyColumns);
                groupbyCols.addAll(funcUsedDimenAsMetric.getColRefs());
                sqlDigest.groupbyColumns = Lists.newArrayList(groupbyCols);
            }
        }

        if (sqlDigest.isRawQuery) {
            dimensions.addAll(sqlDigest.allColumns);
        } else {
            dimensions.addAll(sqlDigest.groupbyColumns);
            dimensions.addAll(sqlDigest.filterColumns);
        }
    }

    private static FunctionDesc findAggrFuncFromDataflowDesc(FunctionDesc aggrFunc, NDataflow dataflow) {
        for (MeasureDesc measure : dataflow.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    private static String toErrorMsg(OLAPContext ctx) {
        StringBuilder buf = new StringBuilder("OLAPContext");
        RealizationCheck checkResult = ctx.realizationCheck;
        for (List<RealizationCheck.IncapableReason> reasons : checkResult.getModelIncapableReasons().values()) {
            for (RealizationCheck.IncapableReason reason : reasons) {
                buf.append(", ").append(reason);
            }
        }
        buf.append(", ").append(ctx.firstTableScan);
        for (JoinDesc join : ctx.joins)
            buf.append(", ").append(join);
        return buf.toString();
    }

    public static Map<String, String> matchJoins(NDataModel model, OLAPContext ctx, boolean partialMatch) {
        Map<String, String> matchUp = Maps.newHashMap();
        TableRef firstTable = ctx.firstTableScan.getTableRef();
        boolean matched;

        if (ctx.isFirstTableLookupTableInModel(model)) {
            // one lookup table
            String modelAlias = model.findFirstTable(firstTable.getTableIdentity()).getAlias();
            matchUp = ImmutableMap.of(firstTable.getAlias(), modelAlias);
            matched = true;
            logger.debug("Context fact table {} matched lookup table in model {}", ctx.firstTableScan.getTableName(),
                    model);
        } else if (ctx.joins.size() != ctx.allTableScans.size() - 1) {
            // has hanging tables
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_BAD_JOIN_SEQUENCE));
            return null;
        } else {
            // normal big joins
            if (ctx.getJoinsGraph() == null) {
                ctx.setJoinsGraph(new JoinsGraph(firstTable, ctx.joins));
            }
            matched = ctx.getJoinsGraph().match(model.getJoinsGraph(), matchUp, partialMatch);
            if (!matched) {
                logger.debug("Context join graph missed model {}, model join graph {}", model, model.getJoinsGraph());
                logger.debug("Missed match nodes - Context {}, Model {}",
                        ctx.getJoinsGraph().unmatched(model.getJoinsGraph()),
                        model.getJoinsGraph().unmatched(ctx.getJoinsGraph()));
            }
        }

        if (!matched) {
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_UNMATCHED_JOIN));
            return null;
        }
        ctx.realizationCheck.addCapableModel(model, matchUp);
        return matchUp;
    }

    public static Map<String, String> matchJoins(NDataModel model, OLAPContext ctx) {
        return matchJoins(model, ctx, KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel());
    }

    private static Multimap<NDataModel, IRealization> makeOrderedModelMap(OLAPContext context) {
        OLAPContext first = context;
        KylinConfig kylinConfig = first.olapSchema.getConfig();
        String projectName = first.olapSchema.getProjectName();
        String factTableName = first.firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = NProjectManager.getInstance(kylinConfig).getRealizationsByTable(projectName,
                factTableName);

        final Multimap<NDataModel, IRealization> mapModelToRealizations = HashMultimap.create();
        for (IRealization real : realizations) {
            if (!real.isReady()) {
                context.realizationCheck.addIncapableCube(real,
                        RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_READY));
                logger.warn("Realization {} is not ready for project {} with fact table {}", real, projectName,
                        factTableName);
                continue;
            }
            mapModelToRealizations.put(real.getModel(), real);
        }

        if (mapModelToRealizations.isEmpty()) {
            logger.error("No realization found for project {} with fact table {}", projectName, factTableName);
        }

        return mapModelToRealizations;
    }

    private static class RealizationCost implements Comparable<RealizationCost> {

        public static final int COST_WEIGHT_MEASURE = 1;
        public static final int COST_WEIGHT_DIMENSION = 10;
        public static final int COST_WEIGHT_INNER_JOIN = 100;

        final int cost;

        public RealizationCost(IRealization real) {

            // ref CubeInstance.getCost()
            int countedDimensionNum;
            countedDimensionNum = real.getAllDimensions().size();
            int c = countedDimensionNum * COST_WEIGHT_DIMENSION + real.getMeasures().size() * COST_WEIGHT_MEASURE;
            for (JoinTableDesc join : real.getModel().getJoinTables()) {
                if (join.getJoin().isInnerJoin())
                    c += COST_WEIGHT_INNER_JOIN;
            }
            this.cost = c;
        }

        @Override
        public int compareTo(RealizationCost o) {
            return this.cost - o.cost;
        }
    }
}
