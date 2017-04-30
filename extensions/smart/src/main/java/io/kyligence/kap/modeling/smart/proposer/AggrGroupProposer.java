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

import java.util.Arrays;
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
        if (!context.hasModelStats() && !context.hasQueryStats()) {
            return;
        }

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
            if (context.hasTableStats()) {
                for (String include : includes) {
                    RowKeyColDesc rowKeyColDesc = CubeDescUtil.getRowKeyColDescByName(workCubeDesc, include);
                    colStatsMap.put(rowKeyColDesc.getColumn(), context.getTableColumnStats(rowKeyColDesc.getColRef()));
                }
            }

            aggGroupCandidates = Lists.newArrayList(includes);

            Collections.sort(aggGroupCandidates, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    long c1 = colStatsMap.get(o1) != null ? colStatsMap.get(o1).getCardinality() : 0;
                    long c2 = colStatsMap.get(o2) != null ? colStatsMap.get(o1).getCardinality() : 0;
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
            // according to table stats
            if (context.hasTableStats()) {
                while (candidatesItr.hasNext()) {
                    String rowKeyColName = candidatesItr.next();
                    if (colStatsMap.get(rowKeyColName) != null && colStatsMap.get(rowKeyColName).getCardinality() <= Constants.DIM_MANDATORY_FORCE_CARDINALITY_MAX) {
                        mandatoryCandidates.add(rowKeyColName);
                        candidatesItr.remove();
                    }
                }
                logger.debug("Added {} mandatory dimension from cardinality.", mandatoryCandidates.size());
            }

            // according to query stats
            if (context.hasQueryStats()) {
                QueryStats queryStats = context.getQueryStats();
                if (queryStats.getTotalQueries() > Constants.DIM_AGG_GROUP_MANDATORY_QUERY_MIN) {
                    for (Map.Entry<String, Integer> appear : queryStats.getAppears().entrySet()) {
                        if (appear.getValue() >= queryStats.getTotalQueries()) {
                            String colName = appear.getKey();
                            mandatoryCandidates.add(colName);
                            aggGroupCandidates.remove(colName);
                        }
                    }
                }
            }
        }

        private void buildWithModelStats() {
            if (context.hasModelStats()) {
                ModelStats modelStats = context.getModelStats();
                if (modelStats.getDoubleColumnCardinality() != null) {
                    for (Map.Entry<String, Long> entry : modelStats.getDoubleColumnCardinality().entrySet()) {
                        if (entry.getKey().equals("SSB.CUSTOMER.C_CITY,SSB.CUSTOMER.C_NATION")) {
                            //                            System.out.println("a");
                        }
                        String[] idxPair = entry.getKey().split(",");
                        String colName1 = idxPair[0].trim();
                        String colName2 = idxPair[1].trim();

                        if (!aggGroupCandidates.contains(colName1) || !aggGroupCandidates.contains(colName2)) {
                            continue;
                        }

                        double cardPair = entry.getValue();

                        double cardCol1 = (double) modelStats.getSingleColumnCardinalityVal(colName1);
                        double cardCol2 = (double) modelStats.getSingleColumnCardinalityVal(colName2);

                        if (cardCol1 < cardCol2 * Constants.DIM_AGG_GROUP_DIFF_MIN || cardCol2 < cardCol1 * Constants.DIM_AGG_GROUP_DIFF_MIN) {
                            // skip due to cardinality diff too big between these 2 columns
                            continue;
                        }

                        double score1 = (cardCol1 / cardPair) * context.getBusinessCoe();
                        double score2 = (cardCol2 / cardPair) * context.getBusinessCoe();

                        boolean equal1 = approxEquals(score1);
                        boolean equal2 = approxEquals(score2);

                        //                        logger.debug("Checking column pair from model stats: column1={}({}), column2={}({}), cardPair={}", colName1, cardCol1, colName2, cardCol2, cardPair);

                        if (equal1 && equal2) {
                            relationJointAggRecorder.add(colName1, score1, colName2, score2);
                            logger.debug("Found relation joint pair from model stats: {}={}, {}={}", colName1, score1, colName2, score2);
                        } else if (!approxEquals(modelStats.getCounter() / cardPair)) {
                            // for hierarchy, need to check if column's cardinality equals to model rows, if so do not consider as hierarchy.
                            if (equal1 && !equal2 && score1 > score2) {
                                hierAggRecorder.add(colName2, colName1);
                                logger.debug("Found hierarchy pair from model stats: {}={}, {}={}", colName2, score2, colName1, score1);
                            } else if (!equal1 && equal2 && score1 < score2) {
                                hierAggRecorder.add(colName1, colName2);
                                logger.debug("Found hierarchy pair from model stats: {}={}, {}={}", colName1, score1, colName2, score2);
                            }
                        }
                    }
                } else {
                    logger.debug("ModelStats not found and skip joint/hierarchy agg group.");
                }
            }
        }

        private void buildWithQueryStats() {
            if (context.hasQueryStats()) {
                QueryStats queryStats = context.getQueryStats();
                Map<String, Integer> appears = queryStats.getAppears();
                Map<SortedSet<String>, Integer> coocurrences = queryStats.getCoocurrences();
                for (Map.Entry<SortedSet<String>, Integer> coocurrence : coocurrences.entrySet()) {
                    String colName1 = coocurrence.getKey().first();
                    String colName2 = coocurrence.getKey().last();

                    if (!aggGroupCandidates.contains(colName1) || !aggGroupCandidates.contains(colName2)) {
                        continue;
                    }

                    double score1 = ((double) coocurrence.getValue()) / ((double) appears.get(colName1)) * context.getBusinessCoe();
                    double score2 = ((double) coocurrence.getValue()) / ((double) appears.get(colName2)) * context.getBusinessCoe();

                    boolean equal1 = approxEquals(score1);
                    boolean equal2 = approxEquals(score2);

                    //                    logger.debug("Checking column pair from query stats: column1={}({}), column2={}({}), cardPair={}", colName1, appears.get(colName1), colName2, appears.get(colName2), coocurrence.getValue());

                    if (equal1 && equal2) {
                        relationJointAggRecorder.add(colName1, score1, colName2, score2);
                        logger.debug("Found relation joint pair from query stats: {}={}, {}={}", colName1, score1, colName2, score2);
                    }
                }
            }
        }

        private void buildSmallJointGroup() {
            if (context.hasTableStats() || context.hasModelStats()) {
                Iterator<String> candidatesItr = aggGroupCandidates.iterator();
                while (candidatesItr.hasNext()) {
                    String rowKeyColName = candidatesItr.next();
                    double cardinality = context.getColumnsCardinality(Lists.newArrayList(rowKeyColName));
                    if (cardinality > 0 && cardinality <= Constants.DIM_JOINT_FORCE_CARDINALITY_COL_MAX) {
                        fragmentRecorder.add(rowKeyColName, cardinality);
                    }
                }
            }
        }

        void build() {
            List<List<String>> resultJoint = Lists.newArrayList();
            List<List<String>> resultHier = Lists.newArrayList();

            if (Constants.DIM_AGG_GROUP_KEEP_LEGACY) {
                // keep old select_rule
                SelectRule selectRule = aggGroup.getSelectRule();
                List<String> mandatoryOld = Arrays.asList(selectRule.mandatory_dims);
                aggGroupCandidates.removeAll(mandatoryOld);
                mandatoryCandidates.addAll(mandatoryOld);

                for (String[] joint_dim : selectRule.joint_dims) {
                    List<String> jointOld = Arrays.asList(joint_dim);
                    aggGroupCandidates.removeAll(jointOld);
                    resultJoint.add(jointOld);
                }
                for (String[] hierarchy_dim : selectRule.hierarchy_dims) {
                    List<String> hierOld = Arrays.asList(hierarchy_dim);
                    aggGroupCandidates.removeAll(hierOld);
                    resultHier.add(hierOld);
                }
            }

            buildMandatory();
            buildWithModelStats();
            buildWithQueryStats();
            buildSmallJointGroup();

            List<List<String>> relationJointList = relationJointAggRecorder.getResult(null);
            List<List<String>> hierList = hierAggRecorder.getResult(relationJointList);
            List<List<String>> fragementJointList = fragmentRecorder.getResult(hierList, relationJointList);

            resultJoint.addAll(relationJointList);
            resultJoint.addAll(fragementJointList);
            resultHier.addAll(hierList);

            SelectRule selectRule = aggGroup.getSelectRule();
            selectRule.mandatory_dims = mandatoryCandidates.toArray(new String[mandatoryCandidates.size()]);
            selectRule.joint_dims = ArrayUtils.to2DArray(resultJoint);
            selectRule.hierarchy_dims = ArrayUtils.to2DArray(resultHier);
        }
    }
}
