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

package io.kyligence.kap.modeling.auto.tuner.step;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.ArrayUtils;
import io.kyligence.kap.modeling.auto.ModelingContext;
import io.kyligence.kap.modeling.auto.tuner.AggGroupScoreUtil;
import io.kyligence.kap.modeling.auto.tuner.model.AbstractAggGroupRecorder;
import io.kyligence.kap.modeling.auto.tuner.model.HierarchyAggGroupRecorder;
import io.kyligence.kap.modeling.auto.tuner.model.JointAggGroupRecorder;
import io.kyligence.kap.modeling.auto.util.Constants;
import io.kyligence.kap.modeling.auto.util.CubeDescUtil;
import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class UpdateAggGroupStep extends AbstractStep {

    private static final Logger logger = LoggerFactory.getLogger(UpdateAggGroupStep.class);

    public UpdateAggGroupStep(ModelingContext context, CubeDesc origCubeDesc, CubeDesc workCubeDesc) {
        super(context, origCubeDesc, workCubeDesc);
    }

    @Override
    public void doOptimize() {
        List<AggregationGroup> aggGroups = workCubeDesc.getAggregationGroups();
        for (AggregationGroup aggGroup : aggGroups) {
            optimizeAggGroup(aggGroup);
        }
    }

    @Override
    void beforeOptimize() throws Exception {
        super.beforeOptimize();

        Preconditions.checkNotNull(workCubeDesc.getAggregationGroups());
        for (AggregationGroup aggGroup : workCubeDesc.getAggregationGroups()) {
            Preconditions.checkNotNull(aggGroup.getIncludes());
        }
    }

    private void optimizeAggGroup(AggregationGroup aggGroup) {
        String[] includes = aggGroup.getIncludes();
        final Map<String, TableExtDesc.ColumnStats> colStatsMap = Maps.newHashMapWithExpectedSize(includes.length);
        final Map<String, String> colIdentityMap = Maps.newHashMapWithExpectedSize(includes.length);
        for (String include : includes) {
            RowKeyColDesc rowKeyColDesc = CubeDescUtil.getRowKeyColDescByName(workCubeDesc, include);
            TableExtDesc.ColumnStats columnStats = context.getRowKeyColumnStats(rowKeyColDesc);
            colStatsMap.put(rowKeyColDesc.getColumn(), columnStats);
            colIdentityMap.put(rowKeyColDesc.getColRef().getCanonicalName(), rowKeyColDesc.getColumn());
        }

        List<String> aggGroupCandidates = Lists.newArrayList(colStatsMap.keySet());
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

        // initialize agg groups
        List<String> mandatoryCandidates = Lists.newArrayList();
        AbstractAggGroupRecorder jointAggRecorder = new JointAggGroupRecorder();
        AbstractAggGroupRecorder hierAggRecorder = new HierarchyAggGroupRecorder();

        // set mandatory agg group
        {
            Iterator<String> candidatesItr = aggGroupCandidates.iterator();
            while (candidatesItr.hasNext()) {
                String rowKeyColName = candidatesItr.next();
                if (colStatsMap.get(rowKeyColName).getCardinality() <= Constants.DIM_MANDATORY_FORCE_CARDINALITY_MAX) {
                    mandatoryCandidates.add(rowKeyColName);
                    candidatesItr.remove();
                }
            }
            logger.debug("Added {} mandatory dimension from cardinality.", mandatoryCandidates.size());
        }

        // set joint and hier according to model stats
        ModelStats modelStats = context.getModelStats();
        if (modelStats != null && modelStats.getDoubleColumnCardinality() != null) {
            for (Map.Entry<String, Long> entry : modelStats.getDoubleColumnCardinality().entrySet()) {
                double cardPair = entry.getValue();

                String[] idxPair = entry.getKey().split(",");
                String colName1 = idxPair[0].trim();
                String colName2 = idxPair[1].trim();

                String colId1 = colIdentityMap.get(colName1);
                String colId2 = colIdentityMap.get(colName2);

                // skip those columns from model but not on cube
                if (!aggGroupCandidates.contains(colId1) || !aggGroupCandidates.contains(colId2)) {
                    continue;
                }

                long cardCol1 = modelStats.getSingleColumnCardinalityVal(colName1);
                long cardCol2 = modelStats.getSingleColumnCardinalityVal(colName2);
                long cardMultiple = cardCol1 * cardCol2;
                double cardRatio = (double) (cardCol1 * cardCol2) / cardPair;
                double hierRatio1 = cardPair / (double) cardCol1;
                double hierRatio2 = cardPair / (double) cardCol2;

                logger.debug("Checking column pair: column1={}({}), column2={}({}), cardPair={}, ratio={}", colName1, cardCol1, colName2, cardCol2, cardMultiple, cardRatio);

                // try hier first: if CD(A,B)=CD(B) then A->B
                if (hierRatio1 > Constants.DIM_AGG_GROUP_HIERARCHY_CORRELATION_COE_MIN && hierRatio1 < Constants.DIM_AGG_GROUP_HIERARCHY_CORRELATION_COE_MAX) {
                    hierAggRecorder.addRecord(Lists.newArrayList(colId2, colId1), AggGroupScoreUtil.scoreHierFromModelStats(hierRatio1) * context.getwPhyscal());
                } else if (hierRatio2 > Constants.DIM_AGG_GROUP_HIERARCHY_CORRELATION_COE_MIN && hierRatio2 < Constants.DIM_AGG_GROUP_HIERARCHY_CORRELATION_COE_MAX) {
                    hierAggRecorder.addRecord(Lists.newArrayList(colId1, colId2), AggGroupScoreUtil.scoreHierFromModelStats(hierRatio2) * context.getwPhyscal());
                }

                if (cardRatio > Constants.DIM_AGG_GROUP_JOINT_CORRELATION_COE_MAX) {
                    jointAggRecorder.addRecord(Lists.newArrayList(colId1, colId2), AggGroupScoreUtil.scoreJointFromModelStats(cardRatio) * context.getwPhyscal());
                }
            }
            logger.debug("Recorded {} hierarchy from model stats.", hierAggRecorder.size());
            logger.debug("Recorded {} joint from model stats.", jointAggRecorder.size());
        } else {
            logger.debug("ModelStats not found and skip joint/hierarchy agg group.");
        }

        // merge small cardinality dimensions to joint group
        {
            int smallMergeJointCnt = 0;
            Iterator<String> candidatesItr = aggGroupCandidates.iterator();
            List<String> smallRowKeyGroup = Lists.newLinkedList();
            while (candidatesItr.hasNext()) {
                String rowKeyColName = candidatesItr.next();
                if (context.getColumnPairCardinality(Lists.newArrayList(rowKeyColName)) > Constants.DIM_JOINT_FORCE_CARDINALITY_GROUP_MAX) {
                    continue;
                }
                smallRowKeyGroup.add(rowKeyColName);

                // TODO: use DP to find most optimal solution
                if (context.getColumnPairCardinality(smallRowKeyGroup) > Constants.DIM_JOINT_FORCE_CARDINALITY_GROUP_MAX || smallRowKeyGroup.size() > Constants.DIM_AGG_GROUP_JOINT_ELEMENTS_MAX) {
                    smallRowKeyGroup.remove(smallRowKeyGroup.size() - 1);
                    if (smallRowKeyGroup.size() > 1) {
                        long groupCard = context.getColumnPairCardinality(smallRowKeyGroup);
                        jointAggRecorder.addRecord(Lists.newArrayList(smallRowKeyGroup), AggGroupScoreUtil.scoreJointFromSmallDims(groupCard) * context.getwPhyscal());
                        smallMergeJointCnt++;
                        logger.debug("Found one small cardinality dimension group: cardinality={}, dimension={}", groupCard, smallRowKeyGroup);
                    }
                    smallRowKeyGroup.clear();
                    smallRowKeyGroup.add(rowKeyColName);
                }
            }

            if (smallRowKeyGroup.size() > 1) {
                long groupCard = context.getColumnPairCardinality(smallRowKeyGroup);
                if (groupCard < Constants.DIM_JOINT_FORCE_CARDINALITY_GROUP_MAX) {
                    jointAggRecorder.addRecord(Lists.newArrayList(smallRowKeyGroup), AggGroupScoreUtil.scoreJointFromSmallDims(groupCard) * context.getwPhyscal());
                    smallMergeJointCnt++;
                    logger.debug("Found one small cardinality dimension group: cardinality={}, dimension={}", groupCard, smallRowKeyGroup);
                }
            }
            logger.debug("Added {} joint from merging small cardinality dimensions.", smallMergeJointCnt);
        }

        // assign new agg groups
        List<List<String>> hierList = hierAggRecorder.getAggGroups();
        List<List<String>> jointList = jointAggRecorder.getAggGroups();
        removeDuplication(jointList, hierList);

        SelectRule selectRule = aggGroup.getSelectRule();
        selectRule.mandatory_dims = mandatoryCandidates.toArray(new String[mandatoryCandidates.size()]);
        selectRule.joint_dims = ArrayUtils.to2DArray(jointList);
        selectRule.hierarchy_dims = ArrayUtils.to2DArray(hierList);
    }

    private void removeDuplication(List<List<String>> minuend, List<List<String>> subtractor) {
        Iterator<List<String>> iter = minuend.iterator();
        while (iter.hasNext()) {
            List<String> curr = iter.next();
            for (List<String> sub : subtractor) {
                if (CollectionUtils.containsAny(curr, sub)) {
                    iter.remove();
                    break;
                }
            }
        }

    }
}
