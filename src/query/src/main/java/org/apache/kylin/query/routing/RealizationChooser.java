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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.cuboid.NLookupCandidate;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;

public class RealizationChooser {

    private static final Logger logger = LoggerFactory.getLogger(RealizationChooser.class);

    // select models for given contexts, return realization candidates for each context
    public static void selectLayoutCandidate(List<OLAPContext> contexts) {
        // try different model for different context
        for (OLAPContext ctx : contexts) {
            ctx.realizationCheck = new RealizationCheck();
            attemptSelectCandidate(ctx);
            Preconditions.checkNotNull(ctx.realization);
        }
    }

    private static void attemptSelectCandidate(OLAPContext context) {
        context.setHasSelected(true);
        // Step 1. match Model,  joins
        Multimap<NDataModel, IRealization> modelMap = makeOrderedModelMap(context);
        if (modelMap.size() == 0) {
            throw new NoRealizationFoundException("No model found for " + toErrorMsg(context));
        }
        List<Candidate> candidates = Lists.newArrayList();
        Map<NDataModel, Map<String, String>> model2AliasMap = Maps.newHashMap();
        for (NDataModel model : modelMap.keySet()) {
            final Map<String, String> map = matchJoins(model, context);
            if (map == null) {
                continue;
            }
            context.fixModel(model, map);
            model2AliasMap.put(model, map);

            // Step 2. select realizations
            preprocessOlapCtx(context);
            Candidate candidate = QueryRouter.selectRealization(context, Sets.newHashSet(modelMap.get(model)));
            if (candidate != null)
                candidates.add(candidate);

            context.unfixModel();
        }

        // Step 3. find the lowest-cost candidate
        Collections.sort(candidates);
        if (!candidates.isEmpty()) {
            Candidate selectedCandidate = candidates.get(0);
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
                        (NLayoutCandidate) selectedCandidate.capability.getSelectedCandidate());
            }
            return;
        }

        throw new NoRealizationFoundException("No realization found for " + toErrorMsg(context));
    }

    private static void adjustForCapabilityInfluence(Candidate chosen, OLAPContext olapContext) {
        CapabilityResult capability = chosen.getCapability();
        for (CapabilityResult.CapabilityInfluence inf : capability.influences) {
            // convert the metric to dimension
            if (inf instanceof CapabilityResult.DimensionAsMeasure) {
                FunctionDesc functionDesc = ((CapabilityResult.DimensionAsMeasure) inf).getMeasureFunction();
                functionDesc.setDimensionAsMetric(true);
                addToContextGroupBy(functionDesc.getParameter().getColRefs(), olapContext);
                olapContext.resetSQLDigest();
                olapContext.getSQLDigest();
                logger.info("Adjust DimensionAsMeasure for " + functionDesc);
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
            if (col.isInnerColumn() == false && context.belongToContextTables(col))
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
                TblColRef col = func.getParameter().getColRefs().get(1);
                context.getGroupByColumns().add(col);
            }
        }
    }

    private static void buildStorageContext(StorageContext context, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, NLayoutCandidate selectedCandidate) {
        LayoutEntity cuboidLayout = selectedCandidate.getCuboidLayout();
        context.setCandidate(selectedCandidate);
        context.setDimensions(dimensions);
        context.setMetrics(metrics);
        context.setCuboidId(cuboidLayout.getId());
        logger.info("Choose model name: {} joins: {}", cuboidLayout.getModel().getUuid(),
                cuboidLayout.getModel().getJoinsGraph().toString());
        logger.info("Choose indexPlan name: {} for context: {}", cuboidLayout.getIndex().getIndexPlan().getUuid(),
                context.getCtxId());
        logger.info("Choose cuboid layout ID: {} dimensions: {}, measures: {}", cuboidLayout.getId(),
                cuboidLayout.getOrderedDimensions(), cuboidLayout.getOrderedMeasures());
    }

    private static void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions,
            Collection<FunctionDesc> metrics, NDataflow dataflow) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric() && !func.isGrouping()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision
                FunctionDesc aggrFuncFromDataflowDesc = dataflow.findAggrFuncFromDataflowDesc(func);
                metrics.add(aggrFuncFromDataflowDesc);
            } else if (func.isDimensionAsMetric()) {
                FunctionDesc funcUsedDimenAsMetric = findAggrFuncFromDataflowDesc(func, dataflow);
                dimensions.addAll(funcUsedDimenAsMetric.getParameter().getColRefs());

                Set<TblColRef> groupbyCols = Sets.newLinkedHashSet(sqlDigest.groupbyColumns);
                groupbyCols.addAll(funcUsedDimenAsMetric.getParameter().getColRefs());
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
        //        for (RealizationCheck.IncapableReason reason : checkResult.getCubeIncapableReasons().values()) {
        //            buf.append(", ").append(reason);
        //        }
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

    public static Map<String, String> matchJoins(NDataModel model, OLAPContext ctx) {
        Map<String, String> matchUp = Maps.newHashMap();
        TableRef firstTable = ctx.firstTableScan.getTableRef();
        boolean matched;

        if (ctx.isFirstTableLookupTableInModel(model)) {
            // one lookup table
            String modelAlias = model.findFirstTable(firstTable.getTableIdentity()).getAlias();
            matchUp = ImmutableMap.of(firstTable.getAlias(), modelAlias);
            matched = true;
        } else if (ctx.joins.size() != ctx.allTableScans.size() - 1) {
            // has hanging tables
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_BAD_JOIN_SEQUENCE));
            //            throw new IllegalStateException("Please adjust the sequence of join tables. " + toErrorMsg(ctx));
            return null;
        } else {
            // normal big joins
            if (ctx.getJoinsGraph() == null) {
                ctx.setJoinsGraph(new JoinsGraph(firstTable, ctx.joins));
            }
            matched = ctx.getJoinsGraph().match(model.getJoinsGraph(), matchUp);
        }

        if (!matched) {
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_UNMATCHED_JOIN));
            return null;
        }
        ctx.realizationCheck.addCapableModel(model, matchUp);
        return matchUp;
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
                continue;
            }
            mapModelToRealizations.put(real.getModel(), real);
        }

        return mapModelToRealizations;
    }

    private static class RealizationCost implements Comparable<RealizationCost> {

        public static final int COST_WEIGHT_MEASURE = 1;
        public static final int COST_WEIGHT_DIMENSION = 10;
        public static final int COST_WEIGHT_INNER_JOIN = 100;

        final public int cost;

        public RealizationCost(IRealization real) {

            // ref CubeInstance.getCost()
            int countedDimensionNum;
            //            if (CubeInstance.REALIZATION_TYPE.equals(real.getType())) {
            //                countedDimensionNum = ((CubeInstance) real).getRowKeyColumnCount();
            //            } else {
            countedDimensionNum = real.getAllDimensions().size();
            //            }
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
