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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.OutOfMaxCombinationException;
import org.apache.kylin.cube.model.TooManyCuboidException;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;

import static org.apache.kylin.common.exception.ServerErrorCode.RULE_BASED_INDEX_METADATA_INCONSISTENT;

@Slf4j
public class NKECuboidScheduler extends NCuboidScheduler {

    private final BigInteger max;
    private final int measureSize;
    private boolean isBaseCuboidValid;
    private boolean skipOrderComparison;
    private final Set<CuboidBigInteger> oldAllCuboidIds;
    private final Set<CuboidBigInteger> newAllCuboidIds;

    private final NewSet newHashSet = () -> new HashSet<>();
    private final NewSet newCuboidSet = () -> new CuboidSet();

    NKECuboidScheduler(IndexPlan indexPlan, NRuleBasedIndex ruleBasedAggIndex) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();
        this.measureSize = ruleBasedAggIndex.getMeasures().size();
        this.isBaseCuboidValid = ruleBasedAggIndex.getIndexPlan().getConfig().isBaseCuboidAlwaysValid();
        this.skipOrderComparison = indexPlan.getConfig().skipRulebasedLayoutsOrderComparsion();

        // handle nRuleBasedCuboidDesc has 0 dimensions
        if (max.bitCount() == 0) {
            oldAllCuboidIds = Sets.newHashSet();
            newAllCuboidIds = new CuboidSet();
        } else {
            this.oldAllCuboidIds = buildTreeBottomUp(newHashSet);
            childParents.clear();
            this.newAllCuboidIds = buildTreeBottomUp(newCuboidSet);
        }
    }

    @Override
    public int getCuboidCount() {
        return oldAllCuboidIds.size();
    }

    @Override
    public List<BigInteger> getAllCuboidIds() {
        val newSortingResult = ((CuboidSet) newAllCuboidIds).getSortedList();
        val oldSortingResult = Sets.newHashSet(oldAllCuboidIds).stream().map(cuboidId -> cuboidId.getDimMeas()).collect(Collectors.toList());

        if (!skipOrderComparison && !newSortingResult.equals(oldSortingResult)) {
            log.error("Index metadata might be inconsistent. Please try refreshing all segments in the following model: Project [{}], Model [{}]",
                    indexPlan.getProject(), indexPlan.getModelAlias(),
                    new KylinException(RULE_BASED_INDEX_METADATA_INCONSISTENT, ""));
        }
        return newSortingResult;
    }

    private Map<CuboidBigInteger, Set<CuboidBigInteger>> childParents = Maps.newConcurrentMap();

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, NewSet newSetFunc) {
        if (childParents.containsKey(child)) {
            return childParents.get(child);
        }
        Set<CuboidBigInteger> parentCandidate = newSetFunc.createNewSet();
        BigInteger childBits = child.getDimMeas();

        if (isBaseCuboidValid && childBits.equals(ruleBasedAggIndex.getFullMask())) {
            return parentCandidate;
        }

        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            if (childBits.equals(agg.getPartialCubeFullMask()) && isBaseCuboidValid) {
                parentCandidate.add(new CuboidBigInteger(ruleBasedAggIndex.getFullMask(), measureSize));
            } else if (childBits.equals(BigInteger.ZERO) || agg.isOnTree(childBits)) {
                parentCandidate.addAll(getOnTreeParents(child, agg, newSetFunc));
            }
        }

        childParents.put(child, parentCandidate);
        return parentCandidate;
    }

    private interface NewSet {
        Set<CuboidBigInteger> createNewSet();
    }

    /**
     * Collect cuboid from bottom up, considering all factor including black list
     * Build tree steps:
     * 1. Build tree from bottom up considering dim capping
     * 2. Kick out blacked-out cuboids from the tree
     *
     * @return Cuboid collection
     */
    private Set<CuboidBigInteger> buildTreeBottomUp(NewSet newSetFunc) {
        int forward = ruleBasedAggIndex.getParentForward();
        KylinConfig config = indexPlan.getConfig();
        long maxCombination = config.getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;

        Set<CuboidBigInteger> cuboidHolder = newSetFunc.createNewSet();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(
                Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)), newSetFunc); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > maxCombination) {
                throw new OutOfMaxCombinationException("Too many cuboids for the cube. Cuboid combination reached "
                        + cuboidHolder.size() + " and limit is " + maxCombination + ". Abort calculation.");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, newSetFunc);
        }

        if (isBaseCuboidValid) {
            cuboidHolder.add(new CuboidBigInteger(ruleBasedAggIndex.getFullMask(), measureSize));
        }

        return cuboidHolder;
    }

    /**
     * Get all parent for children cuboids, considering dim cap.
     *
     * @param children children cuboids
     * @return all parents cuboids
     */
    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children, NewSet newSetFunc) {
        //debugPrint(children, "children");
        Set<CuboidBigInteger> parents = newSetFunc.createNewSet();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, newSetFunc));
        }

        //debugPrint(parents, "parents");
        val filteredParents = Iterators.filter(parents.iterator(), new Predicate<CuboidBigInteger>() {
            @Override
            public boolean apply(@Nullable CuboidBigInteger cuboidId) {
                if (cuboidId == null) {
                    return false;
                }

                BigInteger cuboidBits = cuboidId.getDimMeas();

                if (cuboidBits.equals(ruleBasedAggIndex.getFullMask()) && isBaseCuboidValid) {
                    return true;
                }

                for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
                    if (agg.isOnTree(cuboidBits) && checkDimCap(agg, cuboidBits)) {
                        return true;
                    }
                }

                return false;
            }
        });
        parents = newSetFunc.createNewSet();
        while (filteredParents.hasNext()) {
            parents.add(filteredParents.next());
        }

        //debugPrint(parents, "parents-dimcapped");
        return parents;
    }

    public static void debugPrint(Collection<BigInteger> set, String msg) {
        System.out.println(msg + " ============================================================================");
        for (BigInteger c : set)
            System.out.println(getDisplayName(c, 25));
    }

    private static String getDisplayName(BigInteger cuboidID, int dimensionCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dimensionCount; ++i) {

            if (cuboidID.testBit(i)) {
                sb.append('1');
            } else {
                sb.append('0');
            }
        }
        return StringUtils.reverse(sb.toString());
    }

    /**
     * Get all valid cuboids for agg group, ignoring padding
     *
     * @param agg agg group
     * @return cuboidId list
     */
    @Override
    public List<BigInteger> calculateCuboidsForAggGroup(NAggregationGroup agg) {
        Set<CuboidBigInteger> cuboidHolder = newHashSet.createNewSet();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                agg, newHashSet); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > indexPlan.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new TooManyCuboidException("Holder size larger than kylin.cube.aggrgroup.max-combination");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg, newHashSet);
        }

        return cuboidHolder.stream().map(cuboidBigInteger -> cuboidBigInteger.getDimMeas()).collect(Collectors.toList());
    }

    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children,
            final NAggregationGroup agg, NewSet newSetFunc) {
        Set<CuboidBigInteger> parents = newSetFunc.createNewSet();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, agg, newSetFunc));
        }
        val filteredParent = Iterators.filter(parents.iterator(), new Predicate<CuboidBigInteger>() {
            @Override
            public boolean apply(@Nullable CuboidBigInteger cuboidId) {
                if (cuboidId == null)
                    return false;

                return checkDimCap(agg, cuboidId.getDimMeas());
            }
        });
        parents = newSetFunc.createNewSet();
        while (filteredParent.hasNext()) {
            parents.add(filteredParent.next());
        }
        return parents;
    }

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, NAggregationGroup agg, NewSet newSetFunc) {
        Set<CuboidBigInteger> parentCandidate = newSetFunc.createNewSet();

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

    private boolean checkDimCap(NAggregationGroup agg, BigInteger cuboidID) {
        int dimCap = agg.getDimCap();

        if (dimCap == 0)
            dimCap = ruleBasedAggIndex.getGlobalDimCap();

        if (dimCap <= 0)
            return true;

        int dimCount = 0;

        // mandatory is fixed, thus not counted
        //        if ((cuboidID & agg.getMandatoryColumnMask()) != 0L) {
        //            dimCount++;
        //        }

        for (BigInteger normal : agg.getNormalDimMeas()) {
            if (!(cuboidID.and(normal)).equals(agg.getMeasureMask())) {
                dimCount++;
            }
        }

        for (BigInteger joint : agg.getJoints()) {
            if (!(cuboidID.and(joint)).equals(agg.getMeasureMask()))
                dimCount++;
        }

        for (NAggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            if (!(cuboidID.and(hierarchy.getFullMask())).equals(agg.getMeasureMask()))
                dimCount++;
        }

        return dimCount <= dimCap;
    }
}
