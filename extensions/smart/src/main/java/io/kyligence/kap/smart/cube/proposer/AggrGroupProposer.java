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

package io.kyligence.kap.smart.cube.proposer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.ArrayUtils;
import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.cube.proposer.recorder.FragmentJointAggrGroupRecorder;
import io.kyligence.kap.smart.cube.proposer.recorder.HierarchyAggGroupRecorder;
import io.kyligence.kap.smart.cube.proposer.recorder.RelationJointAggrGroupRecorder;
import io.kyligence.kap.smart.query.QueryStats;
import io.kyligence.kap.smart.query.Utils;
import io.kyligence.kap.smart.util.CubeDescUtil;
import io.kyligence.kap.smart.util.RangeUtil;
import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class AggrGroupProposer extends AbstractCubeProposer {
    private static final Logger logger = LoggerFactory.getLogger(AggrGroupProposer.class);

    public AggrGroupProposer(CubeContext context) {
        super(context);
    }

    @Override
    public void doPropose(CubeDesc workCubeDesc) {
        KylinConfigExt configExt = (KylinConfigExt) workCubeDesc.getConfig();
        Utils.removeCuboidCombinationConf(configExt.getExtendedOverrides());

        // add one default aggregation group if no group exists
        if (workCubeDesc.getAggregationGroups().isEmpty()) {
            CubeDescUtil.fillCubeDefaultAggGroups(workCubeDesc);
        }

        // dimCap is set at cube level on Web UI, so set all groups' dimCap as min value here.
        int minDimCap = 64; // MAX_VALUE
        for (AggregationGroup aggregationGroup : workCubeDesc.getAggregationGroups()) {
            optimizeAggrGroup(workCubeDesc, aggregationGroup);
            if (aggregationGroup.getDimCap() > 0 && aggregationGroup.getDimCap() < minDimCap) {
                minDimCap = aggregationGroup.getDimCap();
            }
        }

        minDimCap = minDimCap > 63 ? 0 : minDimCap;
        for (AggregationGroup aggregationGroup : workCubeDesc.getAggregationGroups()) {
            aggregationGroup.getSelectRule().dimCap = minDimCap;
        }
    }

    private void optimizeAggrGroup(CubeDesc workCubeDesc, AggregationGroup aggGroup) {
        new AggrGroupBuilder(workCubeDesc, aggGroup).build();
    }

    private boolean approxEquals(double score) {
        return RangeUtil.inRange(score, smartConfig.getApproxEqualMin(), smartConfig.getApproxEqualMax());
    }

    private class AggrGroupBuilder {
        final CubeDesc workCubeDesc;
        final Map<String, Long> colCardinalityMap;
        final List<String> aggGroupCandidates;
        final Set<String> mandatoryCandidates = Sets.newHashSet();
        final RelationJointAggrGroupRecorder relationJointAggRecorder = new RelationJointAggrGroupRecorder(smartConfig);
        final HierarchyAggGroupRecorder hierAggRecorder = new HierarchyAggGroupRecorder();
        final FragmentJointAggrGroupRecorder fragmentRecorder = new FragmentJointAggrGroupRecorder(smartConfig);
        AggregationGroup aggGroup;

        private AggrGroupBuilder(CubeDesc workCubeDesc, AggregationGroup aggGroup) {
            this.aggGroup = aggGroup;
            this.workCubeDesc = workCubeDesc;

            String[] includes = aggGroup.getIncludes();
            colCardinalityMap = Maps.newHashMapWithExpectedSize(includes.length);
            if (context.hasTableStats()) {
                for (String include : includes) {
                    RowKeyColDesc rowKeyColDesc = CubeDescUtil.getRowKeyColDescByName(workCubeDesc, include);
                    colCardinalityMap.put(rowKeyColDesc.getColumn(),
                            context.getColumnsCardinality(rowKeyColDesc.getColRef().getIdentity()));
                }
            }

            aggGroupCandidates = Lists.newArrayList(includes);
            Collections.sort(aggGroupCandidates, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    long c1 = colCardinalityMap.get(o1) != null ? colCardinalityMap.get(o1) : 0;
                    long c2 = colCardinalityMap.get(o2) != null ? colCardinalityMap.get(o1) : 0;
                    if (c1 == c2) {
                        return o1.hashCode() - o2.hashCode();
                    } else {
                        return (int) (c1 - c2);
                    }
                }
            });
        }

        private void buildMandatory() {
            if (workCubeDesc.getModel() instanceof KapModel) {
                List<String> mpCols = Arrays
                        .asList(((KapModel) (workCubeDesc.getModel())).getMutiLevelPartitionColStrs());
                mandatoryCandidates.addAll(mpCols);
                aggGroupCandidates.removeAll(mpCols);
            }

            Iterator<String> candidatesItr = aggGroupCandidates.iterator();
            // according to table stats
            if (context.hasTableStats()) {
                while (candidatesItr.hasNext()) {
                    String rowKeyColName = candidatesItr.next();
                    Long cardinality = colCardinalityMap.get(rowKeyColName);
                    if (cardinality != null && cardinality > 0
                            && cardinality <= smartConfig.getMandatoryCardinalityMax()) {
                        mandatoryCandidates.add(rowKeyColName);
                        candidatesItr.remove();
                    }
                }
                logger.trace("Added {} mandatory dimension from cardinality.", mandatoryCandidates.size());
            }

            // according to query stats
            if (context.hasQueryStats()) {
                QueryStats queryStats = context.getQueryStats();
                if (queryStats.getTotalQueries() > smartConfig.getMandatoryEnableQueryMin()) {
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
                        String[] idxPair = entry.getKey().split(",");
                        String colName1 = idxPair[0].trim();
                        String colName2 = idxPair[1].trim();

                        if (!aggGroupCandidates.contains(colName1) || !aggGroupCandidates.contains(colName2)) {
                            continue;
                        }

                        double cardPair = entry.getValue();

                        double cardCol1 = (double) modelStats.getSingleColumnCardinalityVal(colName1);
                        double cardCol2 = (double) modelStats.getSingleColumnCardinalityVal(colName2);

                        if (cardCol1 < cardCol2 * smartConfig.getApproxDiffMax()
                                || cardCol2 < cardCol1 * smartConfig.getApproxDiffMax()) {
                            // skip due to cardinality diff too big between these 2 columns
                            continue;
                        }

                        double score1 = (cardCol1 / cardPair) * smartConfig.getBusinessWeight();
                        double score2 = (cardCol2 / cardPair) * smartConfig.getBusinessWeight();

                        boolean equal1 = approxEquals(score1);
                        boolean equal2 = approxEquals(score2);

                        //                        logger.debug("Checking column pair from model stats: column1={}({}), column2={}({}), cardPair={}", colName1, cardCol1, colName2, cardCol2, cardPair);

                        if (equal1 && equal2) {
                            relationJointAggRecorder.add(colName1, score1, colName2, score2);
                            logger.trace("Found relation joint pair from model stats: {}={}, {}={}", colName1, score1,
                                    colName2, score2);
                        } else if (!approxEquals(modelStats.getCounter() / cardPair)) {
                            // for hierarchy, need to check if column's cardinality equals to model rows, if so do not consider as hierarchy.
                            if (equal1 && !equal2 && score1 > score2) {
                                hierAggRecorder.add(colName2, colName1);
                                logger.trace("Found hierarchy pair from model stats: {}={}, {}={}", colName2, score2,
                                        colName1, score1);
                            } else if (!equal1 && equal2 && score1 < score2) {
                                hierAggRecorder.add(colName1, colName2);
                                logger.trace("Found hierarchy pair from model stats: {}={}, {}={}", colName1, score1,
                                        colName2, score2);
                            }
                        }
                    }
                } else {
                    logger.trace("ModelStats not found and skip joint/hierarchy agg group.");
                }
            }
        }

        private void buildWithQueryStats() {
            if (context.hasQueryStats()) {
                QueryStats queryStats = context.getQueryStats();
                Map<String, Integer> appears = queryStats.getAppears();
                Map<String, Integer> coocurrences = queryStats.getCoocurrences();
                for (Map.Entry<String, Integer> coocurrence : coocurrences.entrySet()) {
                    String[] colPair = coocurrence.getKey().split(",");
                    String colName1 = colPair[0].trim();
                    String colName2 = colPair[1].trim();

                    if (!aggGroupCandidates.contains(colName1) || !aggGroupCandidates.contains(colName2)) {
                        continue;
                    }

                    double score1 = ((double) coocurrence.getValue()) / ((double) appears.get(colName1))
                            * smartConfig.getBusinessWeight();
                    double score2 = ((double) coocurrence.getValue()) / ((double) appears.get(colName2))
                            * smartConfig.getBusinessWeight();

                    boolean equal1 = approxEquals(score1);
                    boolean equal2 = approxEquals(score2);

                    //                    logger.debug("Checking column pair from query stats: column1={}({}), column2={}({}), cardPair={}", colName1, appears.get(colName1), colName2, appears.get(colName2), coocurrence.getValue());

                    if (equal1 && equal2) {
                        relationJointAggRecorder.add(colName1, score1, colName2, score2);
                        logger.trace("Found relation joint pair from query stats: {}={}, {}={}", colName1, score1,
                                colName2, score2);
                    }
                }
            }
        }

        private void buildSmallJointGroup(int retry) {
            if (context.hasTableStats() || context.hasModelStats()) {
                Iterator<String> candidatesItr = aggGroupCandidates.iterator();
                while (candidatesItr.hasNext()) {
                    String rowKeyColName = candidatesItr.next();
                    double cardinality = context.getColumnsCardinality(Lists.newArrayList(rowKeyColName));
                    if (cardinality > 0
                            && cardinality <= smartConfig.getJointGroupCardinalityMax() * Math.pow(10, retry)) {
                        fragmentRecorder.add(rowKeyColName, cardinality);
                    }
                }
                logger.trace("Try to find small joint groups: retry={}", retry);
            }
        }

        private void buildDimCapToControlCuboidNum() {
            if (smartConfig.getAggGroupStrictEnabled() && smartConfig.enableDimCapForAggGroupStrict()) {
                int dimCap = aggGroup.getDimCap();
                if (dimCap > 0) {
                    // if dimCap exists as input, then keep it.
                    return;
                }

                dimCap = aggGroup.getIncludes().length;
                long cuboids = getEstimatedCuboidCombination();
                int retry = 0;
                while (cuboids > smartConfig.getAggGroupStrictCombinationMax()
                        && retry++ < smartConfig.getAggGroupStrictRetryMax() && dimCap > smartConfig.getDimCapMin()) {
                    aggGroup.getSelectRule().dimCap = --dimCap;
                    cuboids = getEstimatedCuboidCombination();
                }
            }
        }

        private long getEstimatedCuboidCombination() {
            try {
                aggGroup.init(workCubeDesc, workCubeDesc.getRowkey());
                return aggGroup.calculateCuboidCombination();
            } catch (Exception e) {
                return Long.MAX_VALUE;
            }
        }

        void build() {
            List<List<String>> resultJoint = Lists.newArrayList();
            List<List<String>> resultHier = Lists.newArrayList();

            if (smartConfig.getAggGroupKeepLegacy()) {
                // keep old select_rule
                SelectRule selectRule = aggGroup.getSelectRule();
                List<String> mandatoryOld = Arrays.asList(selectRule.mandatoryDims);
                aggGroupCandidates.removeAll(mandatoryOld);
                mandatoryCandidates.addAll(mandatoryOld);

                for (String[] joint_dim : selectRule.jointDims) {
                    List<String> jointOld = Arrays.asList(joint_dim);
                    aggGroupCandidates.removeAll(jointOld);
                    resultJoint.add(jointOld);
                }
                for (String[] hierarchy_dim : selectRule.hierarchyDims) {
                    List<String> hierOld = Arrays.asList(hierarchy_dim);
                    aggGroupCandidates.removeAll(hierOld);
                    resultHier.add(hierOld);
                }
            }

            buildMandatory();
            buildWithModelStats();
            buildWithQueryStats();

            List<List<String>> relationJointList = relationJointAggRecorder.getResult(null);
            List<List<String>> hierList = hierAggRecorder.getResult(relationJointList);

            buildSmallJointGroup(0);
            List<List<String>> fragementJointList = fragmentRecorder.getResult(0, hierList, relationJointList);

            resultJoint.addAll(relationJointList);
            resultJoint.addAll(fragementJointList);
            resultHier.addAll(hierList);

            SelectRule selectRule = aggGroup.getSelectRule();
            selectRule.mandatoryDims = mandatoryCandidates.toArray(new String[mandatoryCandidates.size()]);
            selectRule.jointDims = ArrayUtils.to2DArray(resultJoint);
            selectRule.hierarchyDims = ArrayUtils.to2DArray(resultHier);

            buildDimCapToControlCuboidNum();

            if (smartConfig.getAggGroupStrictEnabled() && smartConfig.enableJointForAggGroupStrict()) {
                long cuboidNum = getEstimatedCuboidCombination();
                int retry = 0;
                while (cuboidNum > smartConfig.getAggGroupStrictCombinationMax()
                        && retry++ <= smartConfig.getAggGroupStrictRetryMax()) {
                    resultJoint.removeAll(fragementJointList);

                    buildSmallJointGroup(retry);
                    fragementJointList = fragmentRecorder.getResult(retry, hierList, relationJointList);
                    resultJoint.addAll(fragementJointList);

                    selectRule.jointDims = ArrayUtils.to2DArray(resultJoint);
                    cuboidNum = getEstimatedCuboidCombination();
                }
            }
        }
    }
}
