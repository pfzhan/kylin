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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.cube.model.SelectRule;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.math.LongMath;

import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;

/**
 * to compatible with legacy aggregation group in pre-newten
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NAggregationGroup implements Serializable {
    public static class HierarchyMask implements Serializable {
        public long fullMask; // 00000111
        public long[] allMasks; // 00000100,00000110,00000111
        public long[] dims; // 00000100,00000010,00000001
    }

    @JsonProperty("includes")
    private Integer[] includes;
    @JsonProperty("select_rule")
    private SelectRule selectRule;

    //computed
    private long partialCubeFullMask;
    private long mandatoryColumnMask;
    private List<HierarchyMask> hierarchyMasks;
    private List<Long> joints;//each long is a group
    private long jointDimsMask;
    private List<Long> normalDims;//each long is a single dim
    private NRuleBasedIndex ruleBasedAggIndex;
    private boolean isMandatoryOnlyValid;
    private HashMap<Long, Long> dim2JointMap;

    public void init(NRuleBasedIndex ruleBasedCuboidsDesc) {
        this.ruleBasedAggIndex = ruleBasedCuboidsDesc;
        this.isMandatoryOnlyValid = ruleBasedCuboidsDesc.getIndexPlan().getConfig()
                .getCubeAggrGroupIsMandatoryOnlyValid();

        if (this.includes == null || this.includes.length == 0 || this.selectRule == null) {
            throw new IllegalStateException("AggregationGroup incomplete");
        }

        checkAndNormalizeFields();

        buildPartialCubeFullMask();
        buildMandatoryColumnMask();
        buildJointColumnMask();
        buildJointDimsMask();
        buildHierarchyMasks();
        buildNormalDimsMask();
    }

    private void checkAndNormalizeFields() {
        Preconditions.checkNotNull(includes);
        checkAndNormalizeFields(includes);

        Preconditions.checkNotNull(selectRule.mandatoryDims);
        checkAndNormalizeFields(selectRule.mandatoryDims);

        if (selectRule.hierarchyDims == null)
            selectRule.hierarchyDims = new Integer[0][];
        for (Integer[] cols : selectRule.hierarchyDims) {
            Preconditions.checkNotNull(cols);
            checkAndNormalizeFields(cols);
        }

        if (selectRule.jointDims == null)
            selectRule.jointDims = new Integer[0][];
        for (Integer[] cols : selectRule.jointDims) {
            Preconditions.checkNotNull(cols);
            checkAndNormalizeFields(cols);
        }
    }

    private void checkAndNormalizeFields(Integer[] dims) {
        if (dims == null)
            return;

        // check no dup
        Set<Integer> set = new HashSet<>(Arrays.asList(dims));
        if (set.size() < dims.length)
            throw new IllegalStateException(
                    "Columns in aggrgroup must not contain duplication: " + Arrays.asList(dims));
    }

    private void buildPartialCubeFullMask() {
        Preconditions.checkState(this.includes != null);
        Preconditions.checkState(this.includes.length != 0);

        partialCubeFullMask = 0L;
        for (Integer dimId : this.includes) {
            Integer index = ruleBasedAggIndex.getColumnBitIndex(dimId);
            long bit = 1L << index;
            partialCubeFullMask |= bit;
        }
    }

    private void buildJointColumnMask() {
        joints = Lists.newArrayList();
        dim2JointMap = Maps.newHashMap();

        if (this.selectRule.jointDims == null || this.selectRule.jointDims.length == 0) {
            return;
        }

        for (Integer[] jointDims : this.selectRule.jointDims) {
            if (jointDims == null || jointDims.length == 0) {
                continue;
            }

            long joint = 0L;
            for (int i = 0; i < jointDims.length; i++) {
                Integer index = ruleBasedAggIndex.getColumnBitIndex(jointDims[i]);
                long bit = 1L << index;
                joint |= bit;
            }

            Preconditions.checkState(joint != 0);
            joints.add(joint);
        }

        for (long jt : joints) {
            for (int i = 0; i < 64; i++) {
                if (((1L << i) & jt) != 0) {
                    dim2JointMap.put((1L << i), jt);
                }
            }
        }
    }

    private void buildMandatoryColumnMask() {
        mandatoryColumnMask = 0L;

        Integer[] mandatory_dims = this.selectRule.mandatoryDims;
        if (mandatory_dims == null || mandatory_dims.length == 0) {
            return;
        }

        for (Integer dim : mandatory_dims) {
            Integer index = ruleBasedAggIndex.getColumnBitIndex(dim);
            mandatoryColumnMask |= (1L << index);
        }
    }

    private void buildHierarchyMasks() {
        this.hierarchyMasks = new ArrayList<HierarchyMask>();

        if (this.selectRule.hierarchyDims == null || this.selectRule.hierarchyDims.length == 0) {
            return;
        }

        for (Integer[] hierarchy_dims : this.selectRule.hierarchyDims) {
            HierarchyMask mask = new HierarchyMask();
            if (hierarchy_dims == null || hierarchy_dims.length == 0) {
                continue;
            }

            ArrayList<Long> allMaskList = new ArrayList<Long>();
            ArrayList<Long> dimList = new ArrayList<Long>();
            for (int i = 0; i < hierarchy_dims.length; i++) {
                Integer index = ruleBasedAggIndex.getColumnBitIndex(hierarchy_dims[i]);
                long bit = 1L << index;

                // combine joint as logic dim
                if (dim2JointMap.get(bit) != null) {
                    bit = dim2JointMap.get(bit);
                }

                mask.fullMask |= bit;
                allMaskList.add(mask.fullMask);
                dimList.add(bit);
            }

            Preconditions.checkState(allMaskList.size() == dimList.size());
            mask.allMasks = new long[allMaskList.size()];
            mask.dims = new long[dimList.size()];
            for (int i = 0; i < allMaskList.size(); i++) {
                mask.allMasks[i] = allMaskList.get(i);
                mask.dims[i] = dimList.get(i);
            }

            this.hierarchyMasks.add(mask);
        }
    }

    private void buildNormalDimsMask() {
        //no joint, no hierarchy, no mandatory
        long leftover = partialCubeFullMask & ~mandatoryColumnMask;
        leftover &= ~this.jointDimsMask;
        for (HierarchyMask hierarchyMask : this.hierarchyMasks) {
            leftover &= ~hierarchyMask.fullMask;
        }

        this.normalDims = bits(leftover);
    }

    private List<Long> bits(long x) {
        List<Long> r = Lists.newArrayList();
        long l = x;
        while (l != 0) {
            long bit = Long.lowestOneBit(l);
            r.add(bit);
            l ^= bit;
        }
        return r;
    }

    public void buildJointDimsMask() {
        long ret = 0;
        for (long x : joints) {
            ret |= x;
        }
        this.jointDimsMask = ret;
    }

    public long getMandatoryColumnMask() {
        return mandatoryColumnMask;
    }

    public List<HierarchyMask> getHierarchyMasks() {
        return hierarchyMasks;
    }

    public Long translateToOnTreeCuboid(long cuboidID) {
        if ((cuboidID & ~getPartialCubeFullMask()) > 0) {
            //the partial cube might not contain all required dims
            return null;
        }

        // add mandantory
        cuboidID = cuboidID | getMandatoryColumnMask();

        // add hierarchy
        for (HierarchyMask hierarchyMask : getHierarchyMasks()) {
            long fullMask = hierarchyMask.fullMask;
            long intersect = cuboidID & fullMask;
            if (intersect != 0 && intersect != fullMask) {

                boolean startToFill = false;
                for (int i = hierarchyMask.dims.length - 1; i >= 0; i--) {
                    if (startToFill) {
                        cuboidID |= hierarchyMask.dims[i];
                    } else {
                        if ((cuboidID & hierarchyMask.dims[i]) != 0) {
                            startToFill = true;
                            cuboidID |= hierarchyMask.dims[i];
                        }
                    }
                }
            }
        }

        // add joint dims
        for (Long joint : getJoints()) {
            if (((cuboidID | joint) != cuboidID) && ((cuboidID & ~joint) != cuboidID)) {
                cuboidID = cuboidID | joint;
            }
        }

        if (!isOnTree(cuboidID)) {
            // no column, add one column
            long nonJointDims = removeBits((getPartialCubeFullMask() ^ getMandatoryColumnMask()), getJoints());
            if (nonJointDims != 0) {
                long nonJointNonHierarchy = removeBits(nonJointDims,
                        Collections2.transform(getHierarchyMasks(), new Function<HierarchyMask, Long>() {
                            @Override
                            public Long apply(HierarchyMask input) {
                                return input.fullMask;
                            }
                        }));
                if (nonJointNonHierarchy != 0) {
                    //there exists dim that does not belong to any joint or any hierarchy, that's perfect
                    return cuboidID | Long.lowestOneBit(nonJointNonHierarchy);
                } else {
                    //choose from a hierarchy that does not intersect with any joint dim, only check level 1
                    long allJointDims = jointDimsMask;
                    for (HierarchyMask hierarchyMask : getHierarchyMasks()) {
                        long dim = hierarchyMask.allMasks[0];
                        if ((dim & allJointDims) == 0) {
                            return cuboidID | dim;
                        }
                    }
                }
            }

            cuboidID = cuboidID | Collections.min(getJoints(), NKapCuboidScheduler243.cuboidSelectComparator);
            Preconditions.checkState(isOnTree(cuboidID));
        }
        return cuboidID;
    }

    public long calculateCuboidCombination() {
        long combination = 1;
        try {
            if (this.getDimCap() > 0) {
                NCuboidScheduler cuboidScheduler = ruleBasedAggIndex.getInitialCuboidScheduler();
                combination = cuboidScheduler.calculateCuboidsForAggGroup(this).size();
            } else {
                Set<Integer> includeDims = new TreeSet<>(Arrays.asList(includes));
                Set<Integer> mandatoryDims = new TreeSet<>(Arrays.asList(selectRule.mandatoryDims));

                Set<Integer> hierarchyDims = new TreeSet<>();
                for (Integer[] ss : selectRule.hierarchyDims) {
                    hierarchyDims.addAll(Arrays.asList(ss));
                    combination = LongMath.checkedMultiply(combination, (ss.length + 1));
                }

                Set<Integer> jointDims = new TreeSet<>();
                for (Integer[] ss : selectRule.jointDims) {
                    jointDims.addAll(Arrays.asList(ss));
                }
                combination = LongMath.checkedMultiply(combination, (1L << selectRule.jointDims.length));

                Set<Integer> normalDims = new TreeSet<>(includeDims);
                normalDims.removeAll(mandatoryDims);
                normalDims.removeAll(hierarchyDims);
                normalDims.removeAll(jointDims);

                combination = LongMath.checkedMultiply(combination, (1L << normalDims.size()));

                if (this.isMandatoryOnlyValid && !mandatoryDims.isEmpty()) {
                    combination += 1;
                }
                combination -= 1; // not include cuboid 0
            }
        } catch (ArithmeticException e) {
            combination = Long.MAX_VALUE;
        }

        if (combination < 0)
            combination = Long.MAX_VALUE;

        return combination;
    }

    private long removeBits(long original, Collection<Long> toRemove) {
        long ret = original;
        for (Long joint : toRemove) {
            ret = ret & ~joint;
        }
        return ret;
    }

    public boolean isOnTree(long cuboidID) {
        if (cuboidID <= 0) {
            return false; //cuboid must be greater than 0
        }
        if ((cuboidID & ~partialCubeFullMask) != 0) {
            return false; //a cuboid's parent within agg is at most partialCubeFullMask
        }

        return checkMandatoryColumns(cuboidID) && checkHierarchy(cuboidID) && checkJoint(cuboidID);
    }

    private boolean checkMandatoryColumns(long cuboidID) {
        if ((cuboidID & mandatoryColumnMask) != mandatoryColumnMask) {
            return false;
        } else {
            //base cuboid is always valid
            if (cuboidID == ruleBasedAggIndex.getFullMask()) {
                return true;
            }

            //cuboid with only mandatory columns maybe valid
            return isMandatoryOnlyValid || (cuboidID & ~mandatoryColumnMask) != 0;
        }
    }

    private boolean checkJoint(long cuboidID) {
        for (long joint : joints) {
            long common = cuboidID & joint;
            if (!(common == 0 || common == joint)) {
                return false;
            }
        }
        return true;
    }

    private boolean checkHierarchy(long cuboidID) {
        // if no hierarchy defined in metadata
        if (hierarchyMasks == null || hierarchyMasks.size() == 0) {
            return true;
        }

        for (HierarchyMask hierarchy : hierarchyMasks) {
            long result = cuboidID & hierarchy.fullMask;
            if (result > 0) {
                boolean meetHierarcy = false;
                for (long mask : hierarchy.allMasks) {
                    if (result == mask) {
                        meetHierarcy = true;
                        break;
                    }
                }

                if (!meetHierarcy) {
                    return false;
                }
            }
        }
        return true;
    }

    public List<Long> getJoints() {
        return joints;
    }

    public List<Long> getNormalDims() {
        return normalDims;
    }

    public long getPartialCubeFullMask() {
        return partialCubeFullMask;
    }

    //used by test
    public SelectRule getSelectRule() {
        return selectRule;
    }

    public Integer[] getIncludes() {
        return includes;
    }

    public boolean isMandatoryOnlyValid() {
        return isMandatoryOnlyValid;
    }

    public int getDimCap() {
        return this.selectRule.dimCap == null ? 0 : this.selectRule.dimCap;
    }
}
