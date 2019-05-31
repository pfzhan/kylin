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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.TooManyCuboidException;

import com.google.common.base.Predicate;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;

public class NKapCuboidScheduler243 extends NCuboidScheduler {

    // smaller is better
    public final static Comparator<Long> cuboidSelectComparator = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return ComparisonChain.start().compare(Long.bitCount(o1), Long.bitCount(o2)).compare(o1, o2).result();
        }
    };

    private final long max;
    private final Set<Long> allCuboidIds;
    private final Map<Long, List<Long>> parent2child;

    public NKapCuboidScheduler243(IndexPlan indexPlan) {
        this(indexPlan, null);

    }

    public NKapCuboidScheduler243(IndexPlan indexPlan, NRuleBasedIndex ruleBasedAggIndex) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();

        // handle nRuleBasedCuboidDesc has 0 dimensions
        if (max == 0) {
            allCuboidIds = Sets.newHashSet();
            parent2child = Maps.newHashMap();
        } else {
            Pair<Set<Long>, Map<Long, List<Long>>> pair = buildTreeBottomUp();
            this.allCuboidIds = pair.getFirst();
            this.parent2child = pair.getSecond();
        }
    }

    @Override
    public int getCuboidCount() {
        return allCuboidIds.size();
    }

    @Override
    public List<Long> getSpanningCuboid(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cuboid " + cuboid + " is out of scope 0-" + max);
        }

        List<Long> spanning = parent2child.get(cuboid);
        if (spanning == null) {
            return Collections.EMPTY_LIST;
        }
        return spanning;
    }

    @Override
    public Set<Long> getAllCuboidIds() {
        return Sets.newHashSet(allCuboidIds);
    }

    /**
     * Returns a valid cuboid that best matches the request cuboid.
     */
    @Override
    public long findBestMatchCuboid(long cuboid) {
        if (isValid(cuboid)) {
            return cuboid;
        }

        List<Long> onTreeCandidates = Lists.newArrayList();
        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            Long candidate = agg.translateToOnTreeCuboid(cuboid);
            if (candidate != null) {
                onTreeCandidates.add(candidate);
            }
        }

        if (onTreeCandidates.size() == 0) {
            return getBaseCuboidId();
        }

        long onTreeCandi = Collections.min(onTreeCandidates, cuboidSelectComparator);
        if (isValid(onTreeCandi)) {
            return onTreeCandi;
        }

        return doFindBestMatchCuboid(onTreeCandi);
    }

    private long doFindBestMatchCuboid(long child) {
        long parent = getOnTreeParent(child);
        while (parent > 0) {
            if (ruleBasedAggIndex.getInitialCuboidScheduler().getAllCuboidIds().contains(parent)) {
                break;
            }
            parent = getOnTreeParent(parent);
        }

        if (parent <= 0) {
            throw new IllegalStateException("Can't find valid parent for Cuboid " + child);
        }
        return parent;
    }

    private long getOnTreeParent(long child) {
        Set<Long> candidates = getOnTreeParents(child);
        if (candidates == null || candidates.isEmpty()) {
            return -1;
        }
        return Collections.min(candidates, cuboidSelectComparator);
    }

    private Map<Long, Set<Long>> childParents = Maps.newConcurrentMap();

    private Set<Long> getOnTreeParents(long child) {
        if (childParents.containsKey(child)) {
            return childParents.get(child);
        }
        Set<Long> parentCandidate = new HashSet<>();

        if (child == ruleBasedAggIndex.getFullMask()) {
            return parentCandidate;
        }

        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            if (child == agg.getPartialCubeFullMask()) {
                parentCandidate.add(ruleBasedAggIndex.getFullMask());
            } else if (child == 0 || agg.isOnTree(child)) {
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
    protected Pair<Set<Long>, Map<Long, List<Long>>> buildTreeBottomUp() {
        int forward = ruleBasedAggIndex.getParentForward();
        KylinConfig config = indexPlan.getConfig();
        long maxCombination = config.getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;

        Set<Long> cuboidHolder = new HashSet<>();

        // build tree structure
        Set<Long> children = getOnTreeParentsByLayer(Sets.newHashSet(0L)); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > maxCombination) {
                throw new IllegalStateException("Too many cuboids for the cube. Cuboid combination reached "
                        + cuboidHolder.size() + " and limit is " + maxCombination + ". Abort calculation.");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children);
        }
        cuboidHolder.add(ruleBasedAggIndex.getFullMask());

        // fill padding cuboids
        Map<Long, List<Long>> parent2Child = Maps.newHashMap();
        Queue<Long> cuboidScan = new ArrayDeque<>();
        cuboidScan.addAll(cuboidHolder);
        while (!cuboidScan.isEmpty()) {
            long current = cuboidScan.poll();
            long parent = getParentOnPromise(current, cuboidHolder, forward);

            if (parent > 0) {
                if (!cuboidHolder.contains(parent)) {
                    cuboidScan.add(parent);
                    cuboidHolder.add(parent);
                }
                if (parent2Child.containsKey(parent)) {
                    parent2Child.get(parent).add(current);
                } else {
                    parent2Child.put(parent, Lists.newArrayList(current));
                }
            }
        }

        return Pair.newPair(cuboidHolder, parent2Child);
    }

    private long getParentOnPromise(long child, Set<Long> coll, int forward) {
        long parent = getOnTreeParent(child);
        if (parent < 0) {
            return -1;
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
    private Set<Long> getOnTreeParentsByLayer(Collection<Long> children) {
        //debugPrint(children, "children");
        Set<Long> parents = new HashSet<>();
        for (long child : children) {
            parents.addAll(getOnTreeParents(child));
        }

        //debugPrint(parents, "parents");
        parents = Sets.newHashSet(Iterators.filter(parents.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                if (cuboidId == ruleBasedAggIndex.getFullMask()) {
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

    public static void debugPrint(Collection<Long> set, String msg) {
        System.out.println(msg + " ============================================================================");
        for (Long c : set)
            System.out.println(getDisplayName(c, 25));
    }

    private static String getDisplayName(long cuboidID, int dimensionCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dimensionCount; ++i) {
            if ((cuboidID & (1L << i)) == 0) {
                sb.append('0');
            } else {
                sb.append('1');
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
    public Set<Long> calculateCuboidsForAggGroup(NAggregationGroup agg) {
        Set<Long> cuboidHolder = new HashSet<>();

        // build tree structure
        Set<Long> children = getOnTreeParentsByLayer(Sets.newHashSet(0L), agg); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > indexPlan.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new TooManyCuboidException("Holder size larger than kylin.cube.aggrgroup.max-combination");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg);
        }

        return cuboidHolder;
    }

    private Set<Long> getOnTreeParentsByLayer(Collection<Long> children, final NAggregationGroup agg) {
        Set<Long> parents = new HashSet<>();
        for (long child : children) {
            parents.addAll(getOnTreeParents(child, agg));
        }
        parents = Sets.newHashSet(Iterators.filter(parents.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                return checkDimCap(agg, cuboidId);
            }
        }));
        return parents;
    }

    private Set<Long> getOnTreeParents(long child, NAggregationGroup agg) {
        Set<Long> parentCandidate = new HashSet<>();

        long tmpChild = child;
        if (tmpChild == agg.getPartialCubeFullMask()) {
            return parentCandidate;
        }

        if (agg.getMandatoryColumnMask() != 0L) {
            if (agg.isMandatoryOnlyValid()) {
                if (fillBit(tmpChild, agg.getMandatoryColumnMask(), parentCandidate)) {
                    return parentCandidate;
                }
            } else {
                tmpChild |= agg.getMandatoryColumnMask();
            }
        }

        for (Long normal : agg.getNormalDims()) {
            fillBit(tmpChild, normal, parentCandidate);
        }

        for (Long joint : agg.getJoints()) {
            fillBit(tmpChild, joint, parentCandidate);
        }

        for (NAggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            for (long mask : hierarchy.allMasks) {
                if (fillBit(tmpChild, mask, parentCandidate)) {
                    break;
                }
            }
        }

        return parentCandidate;
    }

    private boolean fillBit(long origin, long other, Set<Long> coll) {
        if ((origin & other) != other) {
            coll.add(origin | other);
            return true;
        }
        return false;
    }

    private boolean checkDimCap(NAggregationGroup agg, long cuboidID) {
        int dimCap = agg.getDimCap();
        if (dimCap <= 0)
            return true;

        int dimCount = 0;

        // mandatory is fixed, thus not counted
        //        if ((cuboidID & agg.getMandatoryColumnMask()) != 0L) {
        //            dimCount++;
        //        }

        for (Long normal : agg.getNormalDims()) {
            if ((cuboidID & normal) != 0L)
                dimCount++;
        }

        for (Long joint : agg.getJoints()) {
            if ((cuboidID & joint) != 0L)
                dimCount++;
        }

        for (NAggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            if ((cuboidID & hierarchy.fullMask) != 0L)
                dimCount++;
        }

        return dimCount <= dimCap;
    }

}
