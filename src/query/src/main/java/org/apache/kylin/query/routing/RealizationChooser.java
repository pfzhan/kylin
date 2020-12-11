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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.alibaba.ttl.TtlRunnable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.NamedThreadFactory;
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
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectLoader;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;


public class RealizationChooser {

    private static final String DATE = "date";
    private static final String TIMESTAMP = "timestamp";
    private static final String VARCHAR = "varchar";
    private static final String STRING = "string";
    private static final String INTEGER = "integer";
    private static final String BIGINT = "bigint";

    private static ExecutorService selectCandidateService = new ThreadPoolExecutor(
            KylinConfig.getInstanceFromEnv().getQueryRealizationChooserThreadCoreNum(),
            KylinConfig.getInstanceFromEnv().getQueryRealizationChooserThreadMaxNum(), 60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(), new NamedThreadFactory("RealizationChooserRunner"),
            new ThreadPoolExecutor.CallerRunsPolicy());

    private static final Logger logger = LoggerFactory.getLogger(RealizationChooser.class);

    private RealizationChooser() {
    }

    // select models for given contexts, return realization candidates for each context
    public static void selectLayoutCandidate(List<OLAPContext> contexts) {
        // try different model for different context
        for (OLAPContext ctx : contexts) {
            if (ctx.isConstantQueryWithAggregations()) {
                continue;
            }
            ctx.realizationCheck = new RealizationCheck();
            attemptSelectCandidate(ctx);
            Preconditions.checkNotNull(ctx.realization);
        }
    }

    public static void multiThreadSelectLayoutCandidate(List<OLAPContext> contexts) {
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            String project = QueryContext.current().getProject();
            // try different model for different context
            CountDownLatch latch = new CountDownLatch(contexts.size());
            ArrayList<Future> futureList = Lists.newArrayList();
            for (OLAPContext ctx : contexts) {
                Future<?> future = selectCandidateService.submit(Objects.requireNonNull(TtlRunnable.get(() -> {
                    try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                            .setAndUnsetThreadLocalConfig(kylinConfig)) {
                        if (project != null) {
                            NTableMetadataManager.getInstance(kylinConfig, project);
                            NDataModelManager.getInstance(kylinConfig, project);
                            NDataflowManager.getInstance(kylinConfig, project);
                            NIndexPlanManager.getInstance(kylinConfig, project);
                            NProjectLoader.updateCache(project);
                        }
                        if (!ctx.isConstantQueryWithAggregations()) {
                            ctx.realizationCheck = new RealizationCheck();
                            attemptSelectCandidate(ctx);
                            Preconditions.checkNotNull(ctx.realization);
                        }
                    } finally {
                        NProjectLoader.removeCache();
                        latch.countDown();
                    }
                })));
                futureList.add(future);
            }
            latch.await();
            for (Future future : futureList) {
                future.get();
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoRealizationFoundException) {
                throw (NoRealizationFoundException) e.getCause();
            }
        } catch (InterruptedException e) {
            logger.error("select layout candidate is interrupted.", e);
            Thread.currentThread().interrupt();
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
        candidates.sort(Candidate.COMPARATOR);
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
                buildStorageContext(context.storageContext, dimensions, metrics, selectedCandidate);
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
                model2AliasMap.get(model));
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
            } else if (FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(func.getExpression())) {
                TblColRef col = func.getColRefs().get(1);
                context.getGroupByColumns().add(col);
            }
        }
    }

    private static void buildStorageContext(StorageContext context, Set<TblColRef> dimensions,
                                            Set<FunctionDesc> metrics, Candidate candidate) {
        val layoutCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        val prunedSegments = candidate.getPrunedSegments();
        val prunedPartitions = candidate.getPrunedPartitions();
        if (layoutCandidate.isEmptyCandidate()) {
            context.setCuboidLayoutId(null);
            context.setEmptyLayout(true);
            logger.info("for context {}, chose empty layout", context.getCtxId());
            return;
        }
        LayoutEntity cuboidLayout = layoutCandidate.getCuboidLayout();
        context.setCandidate(layoutCandidate);
        context.setDimensions(dimensions);
        context.setMetrics(metrics);
        context.setCuboidLayoutId(cuboidLayout.getId());
        context.setPrunedSegments(prunedSegments);
        context.setPrunedPartitions(prunedPartitions);
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
                } else if (FunctionDesc.FUNC_BITMAP_UUID.equalsIgnoreCase(func.getExpression())) {
                    dataflow.getMeasures().stream()
                            .filter(measureDesc -> measureDesc.getFunction().getReturnType().equals("bitmap") && func
                                    .getParameters().get(0).equals(measureDesc.getFunction().getParameters().get(0)))
                            .forEach(measureDesc -> metrics.add(measureDesc.getFunction()));
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
                KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                if (kylinConfig.isJoinMatchOptimizationEnabled()) {
                    logger.debug(
                            "Query match join with join match optimization mode, trying to match with newly rewrite join graph.");
                    ctx.matchJoinWithFilterTransformation();
                    ctx.matchJoinWithEnhancementTransformation();
                    matched = ctx.getJoinsGraph().match(model.getJoinsGraph(), matchUp, partialMatch);
                    logger.debug("Match result for match join with join match optimization mode is: {}", matched);
                }
            }

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