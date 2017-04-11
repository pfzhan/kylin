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

package io.kyligence.kap.modeling.smart.proposer;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.ArrayUtils;
import io.kyligence.kap.modeling.smart.ModelingContext;
import io.kyligence.kap.modeling.smart.proposer.recorder.FragmentJointAggrGroupRecorder;
import io.kyligence.kap.modeling.smart.proposer.recorder.HierarchyAggGroupRecorder;
import io.kyligence.kap.modeling.smart.proposer.recorder.RelationJointAggrGroupRecorder;
import io.kyligence.kap.modeling.smart.query.QueryStats;
import io.kyligence.kap.modeling.smart.util.Constants;
import io.kyligence.kap.modeling.smart.util.CubeDescUtil;
import io.kyligence.kap.modeling.smart.util.RangeUtil;
import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class AggrGroupProposer extends AbstractProposer {
    private static final Logger logger = LoggerFactory.getLogger(AggrGroupProposer.class);

    public AggrGroupProposer(ModelingContext context) {
        super(context);
    }

    @Override
    public void doPropose(CubeDesc workCubeDesc) {
        for (AggregationGroup aggregationGroup : workCubeDesc.getAggregationGroups()) {
            optimizeAggrGroup(workCubeDesc, aggregationGroup);
        }
    }

    private void optimizeAggrGroup(CubeDesc workCubeDesc, AggregationGroup aggGroup) {
        new AggGroupBuilder(workCubeDesc, aggGroup).build();
    }

    private boolean approxEquals(double score) {
        return RangeUtil.inRange(score, Constants.DIM_AGG_GROUP_APPROX_EQUAL_MIN, Constants.DIM_AGG_GROUP_APPROX_EQUAL_MAX);
    }

    private class AggGroupBuilder {
        final CubeDesc workCubeDesc;
        final AggregationGroup aggGroup;
        final Map<String, TableExtDesc.ColumnStats> colStatsMap;
        final Map<String, String> colIdentityMap;
        final List<String> aggGroupCandidates;

        final List<String> mandatoryCandidates = Lists.newArrayList();
        final RelationJointAggrGroupRecorder relationJointAggRecorder = new RelationJointAggrGroupRecorder();
        final HierarchyAggGroupRecorder hierAggRecorder = new HierarchyAggGroupRecorder();
        final FragmentJointAggrGroupRecorder fragmentRecorder = new FragmentJointAggrGroupRecorder();

        private AggGroupBuilder(CubeDesc workCubeDesc, AggregationGroup aggGroup) {
            this.aggGroup = aggGroup;
            this.workCubeDesc = workCubeDesc;

            String[] includes = aggGroup.getIncludes();
            colStatsMap = Maps.newHashMapWithExpectedSize(includes.length);
            colIdentityMap = Maps.newHashMapWithExpectedSize(includes.length); // DEFAULT.FACT.COL => FACT.COL
            for (String include : includes) {
                RowKeyColDesc rowKeyColDesc = CubeDescUtil.getRowKeyColDescByName(workCubeDesc, include);
                TableExtDesc.ColumnStats columnStats = context.getRowKeyColumnStats(rowKeyColDesc);
                colStatsMap.put(rowKeyColDesc.getColumn(), columnStats);
                colIdentityMap.put(rowKeyColDesc.getColRef().getCanonicalName(), rowKeyColDesc.getColumn());
            }

            aggGroupCandidates = Lists.newArrayList(colStatsMap.keySet());
            Collections.sort(aggGroupCandidates, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    long c1 = colStatsMap.get(o1).getCardinality();
                    long c2 = colStatsMap.get(o2).getCardinality();
                    if (c1 == c2) {
                        return o1.hashCode() - o2.hashCode();
                    } else {
                        return (int) (c1 - c2);
                    }
                }
            });
        }

        private void buildMandatory() {
            Iterator<String> candidatesItr = aggGroupCandidates.iterator();
            while (candidatesItr.hasNext()) {
                String rowKeyColName = candidatesItr.next();
                if (colStatsMap.get(rowKeyColName).getCardinality() <= Constants.DIM_MANDATORY_FORCE_CARDINALITY_MAX) {
                    mandatoryCandidates.add(rowKeyColName);
                    candidatesItr.remove();
                }
            }
            logger.debug("Added {} mandatory dimension from cardinality.", mandatoryCandidates.size());

            QueryStats queryStats = context.getQueryStats();
            if (queryStats != null && queryStats.getTotalQueries() > Constants.DIM_AGG_GROUP_MANDATORY_QUERY_MIN) {
                for (Map.Entry<String, Integer> appear : queryStats.getAppears().entrySet()) {
                    if (appear.getValue() >= queryStats.getTotalQueries()) {
                        String colId = colIdentityMap.get(appear.getKey());
                        mandatoryCandidates.add(colId);
                        aggGroupCandidates.remove(colId);
                    }
                }
            }
        }

        private void buildWithModelStats() {
            ModelStats modelStats = context.getModelStats();
            if (modelStats != null && modelStats.getDoubleColumnCardinality() != null) {
                for (Map.Entry<String, Long> entry : modelStats.getDoubleColumnCardinality().entrySet()) {
                    String[] idxPair = entry.getKey().split(",");
                    String colName1 = idxPair[0].trim();
                    String colName2 = idxPair[1].trim();
                    String colId1 = colIdentityMap.get(colName1);
                    String colId2 = colIdentityMap.get(colName2);

                    if (!aggGroupCandidates.contains(colId1) || !aggGroupCandidates.contains(colId2)) {
                        continue;
                    }

                    double cardPair = entry.getValue();

                    double cardCol1 = (double) modelStats.getSingleColumnCardinalityVal(colName1);
                    double cardCol2 = (double) modelStats.getSingleColumnCardinalityVal(colName2);

                    double score1 = (cardCol1 / cardPair) * context.getBusinessCoe();
                    double score2 = (cardCol2 / cardPair) * context.getBusinessCoe();

                    boolean equal1 = approxEquals(score1);
                    boolean equal2 = approxEquals(score2);

                    logger.debug("Checking column pair: column1={}({}), column2={}({}), cardPair={}", colName1, cardCol1, colName2, cardCol2, cardPair);

                    if (equal1 && equal2) {
                        relationJointAggRecorder.add(colId1, score1, colId2, score2);
                    } else if (equal1 && !equal2) {
                        hierAggRecorder.add(colId1, colId2);
                    } else if (!equal1 && equal2) {
                        hierAggRecorder.add(colId2, colId1);
                    }

                    if (equal1 || equal2) {
                        aggGroupCandidates.remove(colId1);
                        aggGroupCandidates.remove(colId2);
                    }
                }
            } else {
                logger.debug("ModelStats not found and skip joint/hierarchy agg group.");
            }
        }

        private void buildWithQueryStats() {
            QueryStats queryStats = context.getQueryStats();
            if (queryStats != null) {
                Map<String, Integer> appears = queryStats.getAppears();
                Map<SortedSet<String>, Integer> coocurrences = queryStats.getCoocurrences();
                for (Map.Entry<SortedSet<String>, Integer> coocurrence : coocurrences.entrySet()) {
                    String colName1 = coocurrence.getKey().first();
                    String colName2 = coocurrence.getKey().last();

                    String colId1 = colIdentityMap.get(colName1);
                    String colId2 = colIdentityMap.get(colName2);

                    if (!aggGroupCandidates.contains(colId1) || !aggGroupCandidates.contains(colId2)) {
                        continue;
                    }

                    double score1 = ((double) coocurrence.getValue()) / ((double) appears.get(colName1)) * context.getBusinessCoe();
                    double score2 = ((double) coocurrence.getValue()) / ((double) appears.get(colName2)) * context.getBusinessCoe();

                    boolean equal1 = approxEquals(score1);
                    boolean equal2 = approxEquals(score2);

                    logger.debug("Checking column pair: column1={}({}), column2={}({}), cardPair={}", colName1, appears.get(colName1), colName2, appears.get(colName2), coocurrence.getValue());

                    if (equal1 && equal2) {
                        relationJointAggRecorder.add(colId1, score1, colId2, score2);
                    } else if (equal1 && !equal2) {
                        hierAggRecorder.add(colId1, colId2);
                    } else if (!equal1 && equal2) {
                        hierAggRecorder.add(colId2, colId1);
                    }

                    if (equal1 || equal2) {
                        aggGroupCandidates.remove(colId1);
                        aggGroupCandidates.remove(colId2);
                    }
                }
            }
        }

        private void buildSmallJointGroup() {
            Iterator<String> candidatesItr = aggGroupCandidates.iterator();
            while (candidatesItr.hasNext()) {
                String rowKeyColName = candidatesItr.next();
                double score = context.getColumnPairCardinality(Lists.newArrayList(rowKeyColName));
                if (score <= Constants.DIM_JOINT_FORCE_CARDINALITY_COL_MAX) {
                    fragmentRecorder.add(rowKeyColName, score);
                }
            }
        }

        void build() {
            buildMandatory();
            buildWithModelStats();
            buildWithQueryStats();
            buildSmallJointGroup();

            List<List<String>> hierList = hierAggRecorder.getResult();
            List<List<String>> jointList = Lists.newArrayList();
            jointList.addAll(relationJointAggRecorder.getResult());
            jointList.addAll(fragmentRecorder.getResult());

            SelectRule selectRule = aggGroup.getSelectRule();
            selectRule.mandatory_dims = mandatoryCandidates.toArray(new String[mandatoryCandidates.size()]);
            selectRule.joint_dims = ArrayUtils.to2DArray(jointList);
            selectRule.hierarchy_dims = ArrayUtils.to2DArray(hierList);
        }
    }
}
