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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.util.JoinsGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
        Map<NDataModel, Set<IRealization>> modelMap = makeOrderedModelMap(context);

        if (modelMap.size() == 0) {
            throw new NoRealizationFoundException("No model found for " + toErrorMsg(context));
        }

        //check all models to collect error message, just for check
        if (BackdoorToggles.getCheckAllModels()) {
            for (Map.Entry<NDataModel, Set<IRealization>> entry : modelMap.entrySet()) {
                final NDataModel model = entry.getKey();
                final Map<String, String> aliasMap = matches(model, context);
                if (aliasMap != null) {
                    context.fixModel(model, aliasMap);
                    QueryRouter.selectRealization(context, entry.getValue());
                    context.unfixModel();
                }
            }
        }

        for (Map.Entry<NDataModel, Set<IRealization>> entry : modelMap.entrySet()) {
            final NDataModel model = entry.getKey();
            final Map<String, String> aliasMap = matches(model, context);
            if (aliasMap != null) {
                context.fixModel(model, aliasMap);

                IRealization realization = QueryRouter.selectRealization(context, entry.getValue());
                if (realization == null) {
                    logger.info("Give up on model {} because no suitable realization is found", model);
                    context.unfixModel();
                    continue;
                }

                context.realization = realization;
                if (!chooseLayout(context)) {
                    context.realization = null;
                    context.unfixModel();
                    logger.info("Give up on realization {} because no suitable layout candidate is found.",
                            realization);
                    continue;
                }
                return; // success
            }
        }

        throw new NoRealizationFoundException("No realization found for " + toErrorMsg(context));

    }

    private static boolean chooseLayout(OLAPContext context) {
        context.resetSQLDigest();
        SQLDigest sqlDigest = context.getSQLDigest();
        NDataflow dataflow = (NDataflow) context.realization;
        // Customized measure taking effect: e.g. allow custom measures to help raw queries
        adjustSqlDigestForAdvanceMeasure(sqlDigest, dataflow);
        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<>();
        Set<FunctionDesc> metrics = new LinkedHashSet<>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics, dataflow);
        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(sqlDigest.filter, filterColumns);

        Segments<NDataSegment> dataSegments = dataflow.getSegments(SegmentStatusEnum.READY);
        // TODO: in future, segment's cuboid may differ
        NLayoutCandidate layoutCandidate = NCuboidLayoutChooser.selectLayoutForQuery(//
                dataSegments.get(0) //
                , ImmutableSet.copyOf(sqlDigest.allColumns) //
                , ImmutableSet.copyOf(dimensions) //
                , ImmutableSet.copyOf(filterColumns) //
                , ImmutableSet.copyOf(metrics) //
                , sqlDigest.isRawQuery); //
        if (layoutCandidate != null) {
            StorageContext storageContext = context.storageContext;
            NCuboidLayout cuboidLayout = layoutCandidate.getCuboidLayout();
            storageContext.setCandidate(layoutCandidate);
            storageContext.setDimensions(dimensions);
            storageContext.setMetrics(metrics);
            logger.info("Choose model name: {}", cuboidLayout.getCuboidDesc().getModel().getName());
            logger.info("Choose cubePlan name: {}", cuboidLayout.getCuboidDesc().getCubePlan().getName());
            logger.info("Choose cuboid layout ID: {} dimensions: {}, measures: {}", cuboidLayout.getId(),
                    cuboidLayout.getOrderedDimensions(),
                    cuboidLayout.getOrderedMeasures());
            return true;
        } else {
            return false;
        }
    }

    private static void adjustSqlDigestForAdvanceMeasure(SQLDigest sqlDigest, NDataflow selectedDataflow) {
        Map<String, List<MeasureDesc>> clazzToMeasuresMap = Maps.newHashMap();
        for (MeasureDesc measure : selectedDataflow.getMeasures()) {
            MeasureType<?> measureType = measure.getFunction().getMeasureType();
            String key = measureType.getClass().getCanonicalName();
            List<MeasureDesc> temp;
            if ((temp = clazzToMeasuresMap.get(key)) != null) {
                temp.add(measure);
            } else {
                clazzToMeasuresMap.put(key, Lists.newArrayList(measure));
            }
        }

        for (List<MeasureDesc> sublist : clazzToMeasuresMap.values()) {
            sublist.get(0).getFunction().getMeasureType().adjustSqlDigest(sublist, sqlDigest);
        }
    }

    private static void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions,
                                                  Collection<FunctionDesc> metrics, NDataflow dataflow) {
        Set<TblColRef> metricColumns = new HashSet<>();
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric() && !func.isGrouping()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision
                FunctionDesc aggrFuncFromDataflowDesc = findAggrFuncFromDataflowDesc(func, dataflow);
                metrics.add(aggrFuncFromDataflowDesc);
                metricColumns.addAll(aggrFuncFromDataflowDesc.getParameter().getColRefs());
            } else if (func.isDimensionAsMetric()) {
                FunctionDesc funcUsedDimenAsMetric = findAggrFuncFromDataflowDesc(func, dataflow);
                dimensions.addAll(funcUsedDimenAsMetric.getParameter().getColRefs());

                Set<TblColRef> groupbyCols = Sets.newLinkedHashSet(sqlDigest.groupbyColumns);
                groupbyCols.addAll(funcUsedDimenAsMetric.getParameter().getColRefs());
                sqlDigest.groupbyColumns = Lists.newArrayList(groupbyCols);
            }
        }
        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (metricColumns.contains(column)
                    && !(sqlDigest.groupbyColumns.contains(column) || sqlDigest.filterColumns.contains(column))) {
                continue;
            }
            dimensions.add(column);
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

    public static Map<String, String> matches(NDataModel model, OLAPContext ctx) {
        Map<String, String> matchUp = Maps.newHashMap();
        TableRef firstTable = ctx.firstTableScan.getTableRef();
        boolean matched;

        if (ctx.joins.isEmpty() && model.isLookupTable(firstTable.getTableIdentity())) {
            // one lookup table
            String modelAlias = model.findFirstTable(firstTable.getTableIdentity()).getAlias();
            matchUp = ImmutableMap.of(firstTable.getAlias(), modelAlias);
            matched = true;
        } else if (ctx.joins.size() != ctx.allTableScans.size() - 1) {
            // has hanging tables
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_BAD_JOIN_SEQUENCE));
            throw new IllegalStateException("Please adjust the sequence of join tables. " + toErrorMsg(ctx));
        } else {
            // normal big joins
            if (ctx.getJoinsGraph() == null) {
                ctx.setJoinsGraph(new JoinsGraph(firstTable, ctx.joins));
            }
            matched = JoinsGraph.match(ctx.getJoinsGraph(), model.getJoinsGraph(), matchUp);
        }

        if (!matched) {
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_UNMATCHED_JOIN));
            return null;
        }
        ctx.realizationCheck.addCapableModel(model, matchUp);
        return matchUp;
    }

    private static Map<NDataModel, Set<IRealization>> makeOrderedModelMap(OLAPContext context) {
        OLAPContext first = context;
        KylinConfig kylinConfig = first.olapSchema.getConfig();
        String projectName = first.olapSchema.getProjectName();
        String factTableName = first.firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = NProjectManager.getInstance(kylinConfig).getRealizationsByTable(projectName,
                factTableName);

        final Map<NDataModel, Set<IRealization>> models = Maps.newHashMap();
        final Map<NDataModel, RealizationCost> costs = Maps.newHashMap();

        for (IRealization real : realizations) {
            if (real.isReady() == false) {
                context.realizationCheck.addIncapableCube(real,
                        RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_READY));
                continue;
            }
            if (containsAll(real.getAllColumnDescs(), first.allColumns) == false) {
                context.realizationCheck.addIncapableCube(real, RealizationCheck.IncapableReason
                        .notContainAllColumn(notContain(real.getAllColumnDescs(), first.allColumns)));
                continue;
            }
            if (RemoveBlackoutRealizationsRule.accept(real) == false) {
                context.realizationCheck.addIncapableCube(real, RealizationCheck.IncapableReason
                        .create(RealizationCheck.IncapableType.CUBE_BLACK_OUT_REALIZATION));
                continue;
            }

            RealizationCost cost = new RealizationCost(real);
            NDataModel m = real.getModel();
            Set<IRealization> set = models.get(m);
            if (set == null) {
                set = Sets.newHashSet();
                set.add(real);
                models.put(m, set);
                costs.put(m, cost);
            } else {
                set.add(real);
                RealizationCost curCost = costs.get(m);
                if (cost.compareTo(curCost) < 0)
                    costs.put(m, cost);
            }
        }

        // order model by cheapest realization cost
        TreeMap<NDataModel, Set<IRealization>> result = Maps.newTreeMap(new Comparator<NDataModel>() {
            @Override
            public int compare(NDataModel o1, NDataModel o2) {
                RealizationCost c1 = costs.get(o1);
                RealizationCost c2 = costs.get(o2);
                int comp = c1.compareTo(c2);
                if (comp == 0)
                    comp = o1.getName().compareTo(o2.getName());
                return comp;
            }
        });
        result.putAll(models);

        return result;
    }

    private static boolean containsAll(Set<ColumnDesc> allColumnDescs, Set<TblColRef> allColumns) {
        for (TblColRef col : allColumns) {
            if (allColumnDescs.contains(col.getColumnDesc()) == false)
                return false;
        }
        return true;
    }

    private static List<TblColRef> notContain(Set<ColumnDesc> allColumnDescs, Set<TblColRef> allColumns) {
        List<TblColRef> notContainCols = Lists.newArrayList();
        for (TblColRef col : allColumns) {
            if (!allColumnDescs.contains(col.getColumnDesc()))
                notContainCols.add(col);
        }
        return notContainCols;
    }

    private static class RealizationCost implements Comparable<RealizationCost> {

        public static final int COST_WEIGHT_MEASURE = 1;
        public static final int COST_WEIGHT_DIMENSION = 10;
        public static final int COST_WEIGHT_INNER_JOIN = 100;

        final public int priority;
        final public int cost;

        public RealizationCost(IRealization real) {
            // ref Candidate.PRIORITIES
            this.priority = Candidate.PRIORITIES.get(real.getType());

            // ref CubeInstance.getCost()
            int countedDimensionNum;
//            if (CubeInstance.REALIZATION_TYPE.equals(real.getType())) {
//                countedDimensionNum = ((CubeInstance) real).getRowKeyColumnCount();
//            } else {
                countedDimensionNum = real.getAllDimensions().size();
//            }
            int c = countedDimensionNum * COST_WEIGHT_DIMENSION
                    + real.getMeasures().size() * COST_WEIGHT_MEASURE;
            for (JoinTableDesc join : real.getModel().getJoinTables()) {
                if (join.getJoin().isInnerJoin())
                    c += COST_WEIGHT_INNER_JOIN;
            }
            this.cost = c;
        }

        @Override
        public int compareTo(RealizationCost o) {
            int comp = this.priority - o.priority;
            if (comp != 0)
                return comp;
            else
                return this.cost - o.cost;
        }
    }
}
