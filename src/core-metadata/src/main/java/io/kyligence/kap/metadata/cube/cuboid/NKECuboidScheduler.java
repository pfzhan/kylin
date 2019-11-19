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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.OutOfMaxCombinationException;
import org.apache.kylin.cube.model.TooManyCuboidException;

import com.google.common.base.Predicate;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;

public class NKECuboidScheduler extends NCuboidScheduler {

    // smaller is better
    final static Comparator<BigInteger> cuboidSelectComparator = (o1, o2) -> ComparisonChain.start()
            .compare(o1.bitCount(), o2.bitCount()).compare(o1, o2).result();

    private final BigInteger max;
    private final Set<BigInteger> allCuboidIds;

    NKECuboidScheduler(IndexPlan indexPlan, NRuleBasedIndex ruleBasedAggIndex) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();

        // handle nRuleBasedCuboidDesc has 0 dimensions
        if (max.bitCount() == 0) {
            allCuboidIds = Sets.newHashSet();
        } else {
            this.allCuboidIds = buildTreeBottomUp();
        }
    }

    @Override
    public int getCuboidCount() {
        return allCuboidIds.size();
    }

    @Override
    public Set<BigInteger> getAllCuboidIds() {
        return Sets.newHashSet(allCuboidIds);
    }

    private BigInteger getOnTreeParent(BigInteger child) {
        Set<BigInteger> candidates = getOnTreeParents(child);
        if (candidates == null || candidates.isEmpty()) {
            return BigInteger.valueOf(-1);
        }
        return Collections.min(candidates, cuboidSelectComparator);
    }

    private Map<BigInteger, Set<BigInteger>> childParents = Maps.newConcurrentMap();

    private Set<BigInteger> getOnTreeParents(BigInteger child) {
        if (childParents.containsKey(child)) {
            return childParents.get(child);
        }
        Set<BigInteger> parentCandidate = new HashSet<>();

        if (child.equals(ruleBasedAggIndex.getFullMask())) {
            return parentCandidate;
        }

        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            if (child.equals(agg.getPartialCubeFullMask())) {
                parentCandidate.add(ruleBasedAggIndex.getFullMask());
            } else if (child.equals(BigInteger.ZERO) || agg.isOnTree(child)) {
                parentCandidate.addAll(getOnTreeParents(child, agg));
            }
        }

        childParents.put(child, parentCandidate);
        return parentCandidate;
    }

    /**
     * Collect cuboid from bottom up, considering all factor including black list
     * Build tree steps:
     * 1. Build tree from bottom up considering dim capping
     * 2. Kick out blacked-out cuboids from the tree
     * 3. Make sure all the cuboids have proper "parent", if not add it to the tree.
     * Direct parent is not necessary, can jump *forward* steps to find in-direct parent.
     * For example, forward = 1, grandparent can also be the parent. Only if both parent
     * and grandparent are missing, add grandparent to the tree.
     *
     * @return Cuboid collection
     */
    private Set<BigInteger> buildTreeBottomUp() {
        int forward = ruleBasedAggIndex.getParentForward();
        KylinConfig config = indexPlan.getConfig();
        long maxCombination = config.getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;

        Set<BigInteger> cuboidHolder = new HashSet<>();

        // build tree structure
        Set<BigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(BigInteger.ZERO)); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > maxCombination) {
                throw new OutOfMaxCombinationException("Too many cuboids for the cube. Cuboid combination reached "
                        + cuboidHolder.size() + " and limit is " + maxCombination + ". Abort calculation.");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children);
        }
        cuboidHolder.add(ruleBasedAggIndex.getFullMask());

        // fill padding cuboids
        Queue<BigInteger> cuboidScan = new ArrayDeque<>();
        cuboidScan.addAll(cuboidHolder);
        while (!cuboidScan.isEmpty()) {
            BigInteger current = cuboidScan.poll();
            BigInteger parent = getParentOnPromise(current, cuboidHolder, forward);

            if (parent.compareTo(BigInteger.ZERO) > 0) {
                if (!cuboidHolder.contains(parent)) {
                    cuboidScan.add(parent);
                    cuboidHolder.add(parent);
                }
            }
        }

        return cuboidHolder;
    }

    private BigInteger getParentOnPromise(BigInteger child, Set<BigInteger> coll, int forward) {
        BigInteger parent = getOnTreeParent(child);
        if (parent.compareTo(BigInteger.ZERO) < 0) {
            return BigInteger.valueOf(-1);
        }

        if (coll.contains(parent) || forward == 0) {
            return parent;
        }

        return getParentOnPromise(parent, coll, forward - 1);
    }

    /**
     * Get all parent for children cuboids, considering dim cap.
     *
     * @param children children cuboids
     * @return all parents cuboids
     */
    private Set<BigInteger> getOnTreeParentsByLayer(Collection<BigInteger> children) {
        //debugPrint(children, "children");
        Set<BigInteger> parents = new HashSet<>();
        for (BigInteger child : children) {
            parents.addAll(getOnTreeParents(child));
        }

        //debugPrint(parents, "parents");
        parents = Sets.newHashSet(Iterators.filter(parents.iterator(), new Predicate<BigInteger>() {
            @Override
            public boolean apply(@Nullable BigInteger cuboidId) {
                if (cuboidId.equals(ruleBasedAggIndex.getFullMask())) {
                    return true;
                }

                for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
                    if (agg.isOnTree(cuboidId) && checkDimCap(agg, cuboidId)) {
                        return true;
                    }
                }

                return false;
            }
        }));

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
    public Set<BigInteger> calculateCuboidsForAggGroup(NAggregationGroup agg) {
        Set<BigInteger> cuboidHolder = new HashSet<>();

        // build tree structure
        Set<BigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(BigInteger.ZERO), agg); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > indexPlan.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new TooManyCuboidException("Holder size larger than kylin.cube.aggrgroup.max-combination");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg);
        }

        return cuboidHolder;
    }

    private Set<BigInteger> getOnTreeParentsByLayer(Collection<BigInteger> children, final NAggregationGroup agg) {
        Set<BigInteger> parents = new HashSet<>();
        for (BigInteger child : children) {
            parents.addAll(getOnTreeParents(child, agg));
        }
        parents = Sets.newHashSet(Iterators.filter(parents.iterator(), new Predicate<BigInteger>() {
            @Override
            public boolean apply(@Nullable BigInteger cuboidId) {
                return checkDimCap(agg, cuboidId);
            }
        }));
        return parents;
    }

    private Set<BigInteger> getOnTreeParents(BigInteger child, NAggregationGroup agg) {
        Set<BigInteger> parentCandidate = new HashSet<>();

        BigInteger tmpChild = child;
        if (tmpChild.equals(agg.getPartialCubeFullMask())) {
            return parentCandidate;
        }

        if (!agg.getMandatoryColumnMask().equals(BigInteger.ZERO)) {
            if (agg.isMandatoryOnlyValid()) {
                if (fillBit(tmpChild, agg.getMandatoryColumnMask(), parentCandidate)) {
                    return parentCandidate;
                }
            } else {
                tmpChild = tmpChild.or(agg.getMandatoryColumnMask());
            }
        }

        for (BigInteger normal : agg.getNormalDims()) {
            fillBit(tmpChild, normal, parentCandidate);
        }

        for (BigInteger joint : agg.getJoints()) {
            fillBit(tmpChild, joint, parentCandidate);
        }

        for (NAggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            for (BigInteger mask : hierarchy.allMasks) {
                if (fillBit(tmpChild, mask, parentCandidate)) {
                    break;
                }
            }
        }

        return parentCandidate;
    }

    private boolean fillBit(BigInteger origin, BigInteger other, Set<BigInteger> coll) {
        if (!(origin.and(other)).equals(other)) {
            coll.add(origin.or(other));
            return true;
        }
        return false;
    }

    private boolean checkDimCap(NAggregationGroup agg, BigInteger cuboidID) {
        int dimCap = agg.getDimCap();
        if (dimCap <= 0)
            return true;

        int dimCount = 0;

        // mandatory is fixed, thus not counted
        //        if ((cuboidID & agg.getMandatoryColumnMask()) != 0L) {
        //            dimCount++;
        //        }

        for (BigInteger normal : agg.getNormalDims()) {
            if (!(cuboidID.and(normal)).equals(BigInteger.ZERO)) {
                dimCount++;
            }
        }

        for (BigInteger joint : agg.getJoints()) {
            if (!(cuboidID.and(joint)).equals(BigInteger.ZERO))
                dimCount++;
        }

        for (NAggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            if (!(cuboidID.and(hierarchy.fullMask)).equals(BigInteger.ZERO))
                dimCount++;
        }

        return dimCount <= dimCap;
    }

}
