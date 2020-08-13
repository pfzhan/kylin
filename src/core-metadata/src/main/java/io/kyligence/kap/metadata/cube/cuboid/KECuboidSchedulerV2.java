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

package io.kyligence.kap.metadata.cube.cuboid;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.exception.OutOfMaxCombinationException;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KECuboidSchedulerV2 extends CuboidScheduler {

    private final BigInteger max;
    private final int measureSize;
    private transient final OrderedSet<ColOrder> allColOrders;

    KECuboidSchedulerV2(IndexPlan indexPlan, NRuleBasedIndex ruleBasedAggIndex, boolean skipAll) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();
        this.measureSize = ruleBasedAggIndex.getMeasures().size();
        boolean isBaseCuboidValid = ruleBasedAggIndex.getIndexPlan().getConfig().isBaseCuboidAlwaysValid();

        // handle nRuleBasedCuboidDesc has 0 dimensions
        allColOrders = new OrderedSet<>();
        if (max.bitCount() == 0 || skipAll) {
            return;
        }
        long maxCombination = indexPlan.getConfig().getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;
        if (isBaseCuboidValid) {
            allColOrders.add(new ColOrder(ruleBasedAggIndex.getDimensions(), ruleBasedAggIndex.getMeasures()));
        }
        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            allColOrders.addAll(calculateCuboidsForAggGroup(agg));
            if (allColOrders.size() > maxCombination) {
                throw new OutOfMaxCombinationException(
                        String.format(OUT_OF_MAX_COMBINATION_MSG_FORMAT, allColOrders.size(), maxCombination));
            }
        }
    }

    @Override
    public int getCuboidCount() {
        return allColOrders.size();
    }

    @Override
    public void validateOrder() {
    }

    @Override
    public void updateOrder() {
    }

    @Override
    public List<ColOrder> getAllColOrders() {
        return allColOrders.getSortedList();
    }

    /**
     * Get all valid cuboids for agg group, ignoring padding
     *
     * @param agg agg group
     * @return cuboidId list
     */
    @Override
    public List<ColOrder> calculateCuboidsForAggGroup(NAggregationGroup agg) {
        Set<CuboidBigInteger> cuboidHolder = new OrderedSet<>();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                agg); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() + children.size() > indexPlan.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new OutOfMaxCombinationException("Holder size larger than kylin.cube.aggrgroup.max-combination");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg);
        }

        return cuboidHolder.stream().map(c -> {
            val colOrder = extractDimAndMeaFromBigInt(c.getDimMeas());
            colOrder.getDimensions().sort(Comparator.comparingInt(x -> ArrayUtils.indexOf(agg.getIncludes(), x)));
            return colOrder;
        }).collect(Collectors.toList());
    }

    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children,
            final NAggregationGroup agg) {
        Set<CuboidBigInteger> parents = new OrderedSet<>();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, agg));
        }
        val filteredParent = Iterators.filter(parents.iterator(), cuboidId -> {
            if (cuboidId == null)
                return false;

            return agg.checkDimCap(cuboidId.getDimMeas());
        });
        parents = new OrderedSet<>();
        while (filteredParent.hasNext()) {
            parents.add(filteredParent.next());
        }
        return parents;
    }

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, NAggregationGroup agg) {
        Set<CuboidBigInteger> parentCandidate = new OrderedSet<>();

        BigInteger tmpChild = child.getDimMeas();
        if (tmpChild.equals(agg.getPartialCubeFullMask())) {
            return parentCandidate;
        }

        if (!agg.getMandatoryColumnMask().equals(agg.getMeasureMask())) {
            if (agg.isMandatoryOnlyValid()) {
                if (fillBit(tmpChild, agg.getMandatoryColumnMask(), parentCandidate)) {
                    return parentCandidate;
                }
            } else {
                tmpChild = tmpChild.or(agg.getMandatoryColumnMask());
            }
        }

        for (BigInteger normal : agg.getNormalDimMeas()) {
            fillBit(tmpChild, normal, parentCandidate);
        }

        for (BigInteger joint : agg.getJoints()) {
            fillBit(tmpChild, joint, parentCandidate);
        }

        for (NAggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            for (BigInteger mask : hierarchy.getAllMasks()) {
                if (fillBit(tmpChild, mask, parentCandidate)) {
                    break;
                }
            }
        }

        return parentCandidate;
    }

    private boolean fillBit(BigInteger origin, BigInteger other, Set<CuboidBigInteger> coll) {
        // if origin contains does not all elements in other
        if (!(origin.and(other)).equals(other)) {
            coll.add(new CuboidBigInteger(origin.or(other), measureSize));
            return true;
        }
        return false;
    }

}
