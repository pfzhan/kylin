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

import static org.apache.kylin.common.exception.ServerErrorCode.RULE_BASED_INDEX_METADATA_INCONSISTENT;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

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
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NKECuboidScheduler extends NCuboidScheduler {

    public static final String INDEX_SCHEDULER_KEY = "kylin.index.rule-scheduler-data";
    private final BigInteger max;
    private final int measureSize;
    private boolean isBaseCuboidValid;
    private transient final Set<CuboidBigInteger> allCuboidIds;

    private transient final SetCreator newHashSet = HashSet::new;
    private transient final SetCreator newCuboidSet = CuboidSet::new;

    NKECuboidScheduler(IndexPlan indexPlan, NRuleBasedIndex ruleBasedAggIndex) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();
        this.measureSize = ruleBasedAggIndex.getMeasures().size();
        this.isBaseCuboidValid = ruleBasedAggIndex.getIndexPlan().getConfig().isBaseCuboidAlwaysValid();

        // handle nRuleBasedCuboidDesc has 0 dimensions
        if (max.bitCount() == 0) {
            allCuboidIds = new CuboidSet();
        } else {
            this.allCuboidIds = buildTreeBottomUp(newCuboidSet);
        }
    }

    @Override
    public int getCuboidCount() {
        return allCuboidIds.size();
    }

    @Override
    public void validateOrder() {
        val newSortingResult = ((CuboidSet) allCuboidIds).getSortedList();
        val data = indexPlan.getOverrideProps().get(INDEX_SCHEDULER_KEY);
        List<BigInteger> oldSortingResult;
        if (StringUtils.isEmpty(data)) {
            Set<CuboidBigInteger> oldAllCuboidIds = Sets.newHashSet();
            if (max.bitCount() > 0) {
                childParents.clear();
                oldAllCuboidIds = buildTreeBottomUp(newHashSet);
            }
            oldSortingResult = Sets.newHashSet(oldAllCuboidIds).stream().map(CuboidBigInteger::getDimMeas)
                    .collect(Collectors.toList());
        } else {
            oldSortingResult = Stream.of(StringUtils.split(data, ",")).map(BigInteger::new)
                    .collect(Collectors.toList());
        }
        if (!newSortingResult.equals(oldSortingResult)) {
            log.error(
                    "Index metadata might be inconsistent. Please try refreshing all segments in the following model: Project [{}], Model [{}]",
                    indexPlan.getProject(), indexPlan.getModelAlias(),
                    new KylinException(RULE_BASED_INDEX_METADATA_INCONSISTENT, ""));
            log.debug("Set difference new:{}, old:{}", newSortingResult, oldSortingResult);
        }

    }

    @Override
    public void updateOrder() {
        val newSortingResult = ((CuboidSet) allCuboidIds).getSortedList();
        indexPlan.getOverrideProps().put(NKECuboidScheduler.INDEX_SCHEDULER_KEY,
                StringUtils.join(newSortingResult, ","));
    }

    @Override
    public List<BigInteger> getAllCuboidIds() {
        return ((CuboidSet) allCuboidIds).getSortedList();
    }

    private Map<CuboidBigInteger, Set<CuboidBigInteger>> childParents = Maps.newConcurrentMap();

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, SetCreator setCreatorFunc) {
        if (childParents.containsKey(child)) {
            return childParents.get(child);
        }
        Set<CuboidBigInteger> parentCandidate = setCreatorFunc.create();
        BigInteger childBits = child.getDimMeas();

        if (isBaseCuboidValid && childBits.equals(ruleBasedAggIndex.getFullMask())) {
            return parentCandidate;
        }

        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            if (childBits.equals(agg.getPartialCubeFullMask()) && isBaseCuboidValid) {
                parentCandidate.add(new CuboidBigInteger(ruleBasedAggIndex.getFullMask(), measureSize));
            } else if (childBits.equals(BigInteger.ZERO) || agg.isOnTree(childBits)) {
                parentCandidate.addAll(getOnTreeParents(child, agg, setCreatorFunc));
            }
        }

        childParents.put(child, parentCandidate);
        return parentCandidate;
    }

    private interface SetCreator {
        Set<CuboidBigInteger> create();
    }

    /**
     * Collect cuboid from bottom up, considering all factor including black list
     * Build tree steps:
     * 1. Build tree from bottom up considering dim capping
     * 2. Kick out blacked-out cuboids from the tree
     *
     * @return Cuboid collection
     */
    private Set<CuboidBigInteger> buildTreeBottomUp(SetCreator setCreatorFunc) {
        int forward = ruleBasedAggIndex.getParentForward();
        KylinConfig config = indexPlan.getConfig();
        long maxCombination = config.getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;

        Set<CuboidBigInteger> cuboidHolder = setCreatorFunc.create();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                setCreatorFunc); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > maxCombination) {
                throw new OutOfMaxCombinationException("Too many cuboids for the cube. Cuboid combination reached "
                        + cuboidHolder.size() + " and limit is " + maxCombination + ". Abort calculation.");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, setCreatorFunc);
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
    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children,
            SetCreator setCreatorFunc) {
        //debugPrint(children, "children");
        Set<CuboidBigInteger> parents = setCreatorFunc.create();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, setCreatorFunc));
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
        parents = setCreatorFunc.create();
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
        Set<CuboidBigInteger> cuboidHolder = newHashSet.create();

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

        return cuboidHolder.stream().map(cuboidBigInteger -> cuboidBigInteger.getDimMeas())
                .collect(Collectors.toList());
    }

    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children,
            final NAggregationGroup agg, SetCreator setCreatorFunc) {
        Set<CuboidBigInteger> parents = setCreatorFunc.create();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, agg, setCreatorFunc));
        }
        val filteredParent = Iterators.filter(parents.iterator(), new Predicate<CuboidBigInteger>() {
            @Override
            public boolean apply(@Nullable CuboidBigInteger cuboidId) {
                if (cuboidId == null)
                    return false;

                return checkDimCap(agg, cuboidId.getDimMeas());
            }
        });
        parents = setCreatorFunc.create();
        while (filteredParent.hasNext()) {
            parents.add(filteredParent.next());
        }
        return parents;
    }

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, NAggregationGroup agg,
            SetCreator setCreatorFunc) {
        Set<CuboidBigInteger> parentCandidate = setCreatorFunc.create();

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
