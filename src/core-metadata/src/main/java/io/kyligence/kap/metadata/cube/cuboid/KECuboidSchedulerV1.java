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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KECuboidSchedulerV1 extends CuboidScheduler {

    public static final String INDEX_SCHEDULER_KEY = "kylin.index.rule-scheduler-data";
    private final BigInteger max;
    private final int measureSize;
    private final boolean isBaseCuboidValid;
    private transient final Set<CuboidBigInteger> allCuboidIds;

    private transient final SetCreator newHashSet = HashSet::new;
    private transient final SetCreator newCuboidSet = OrderedSet::new;

    KECuboidSchedulerV1(IndexPlan indexPlan, RuleBasedIndex ruleBasedAggIndex, boolean skipAllCuboids) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();
        this.measureSize = ruleBasedAggIndex.getMeasures().size();
        this.isBaseCuboidValid = ruleBasedAggIndex.getIndexPlan().getConfig().isBaseCuboidAlwaysValid();
        if (skipAllCuboids) {
            this.allCuboidIds = new OrderedSet<>();
            return;
        }

        // handle nRuleBasedCuboidDesc has 0 dimensions
        if (max.bitCount() == 0) {
            allCuboidIds = new OrderedSet<>();
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
        val newSortingResult = ((OrderedSet) allCuboidIds).getSortedList();
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
        val newSortingResult = ((OrderedSet<CuboidBigInteger>) allCuboidIds).getSortedList();
        indexPlan.getOverrideProps().put(KECuboidSchedulerV1.INDEX_SCHEDULER_KEY,
                StringUtils.join(newSortingResult, ","));
    }

    @Override
    public List<ColOrder> getAllColOrders() {
        return ((OrderedSet<CuboidBigInteger>) allCuboidIds).getSortedList().stream()
                .map(c -> extractDimAndMeaFromBigInt(c.getDimMeas())).collect(Collectors.toList());
    }

    private final Map<CuboidBigInteger, Set<CuboidBigInteger>> childParents = Maps.newConcurrentMap();

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
        KylinConfig config = indexPlan.getConfig();
        long maxCombination = config.getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;

        Set<CuboidBigInteger> cuboidHolder = setCreatorFunc.create();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                setCreatorFunc, maxCombination); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() + children.size() > maxCombination) {
                throw new OutOfMaxCombinationException(String.format(OUT_OF_MAX_COMBINATION_MSG_FORMAT,
                        cuboidHolder.size() + children.size(), maxCombination));
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, setCreatorFunc, maxCombination);
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
            SetCreator setCreatorFunc, long maxCombination) {
        Set<CuboidBigInteger> parents = setCreatorFunc.create();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, setCreatorFunc));
        }

        //debugPrint(parents, "parents");
        val filteredParents = Iterators.filter(parents.iterator(), new Predicate<CuboidBigInteger>() {
            private int cuboidCount = 0;

            @Override
            public boolean apply(@Nullable CuboidBigInteger cuboidId) {
                if (cuboidId == null) {
                    return false;
                }
                if (++cuboidCount > maxCombination) {
                    throw new OutOfMaxCombinationException(
                            String.format(OUT_OF_MAX_COMBINATION_MSG_FORMAT, cuboidCount, maxCombination));
                }

                BigInteger cuboidBits = cuboidId.getDimMeas();

                if (cuboidBits.equals(ruleBasedAggIndex.getFullMask()) && isBaseCuboidValid) {
                    return true;
                }

                for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
                    if (agg.isOnTree(cuboidBits) && agg.checkDimCap(cuboidBits)) {
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

    /**
     * Get all valid cuboids for agg group, ignoring padding
     *
     * @param agg agg group
     * @return cuboidId list
     */
    @Override
    public List<ColOrder> calculateCuboidsForAggGroup(NAggregationGroup agg) {
        Set<CuboidBigInteger> cuboidHolder = newHashSet.create();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                agg, newHashSet); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() + children.size() > indexPlan.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new OutOfMaxCombinationException("Holder size larger than kylin.cube.aggrgroup.max-combination");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg, newHashSet);
        }

        return cuboidHolder.stream().map(c -> extractDimAndMeaFromBigInt(c.getDimMeas())).collect(Collectors.toList());
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

                return agg.checkDimCap(cuboidId.getDimMeas());
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

}
