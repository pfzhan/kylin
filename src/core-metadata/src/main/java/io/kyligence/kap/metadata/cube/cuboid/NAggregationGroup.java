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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.cube.model.SelectRule;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.math.LongMath;

import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import lombok.Getter;
import lombok.Setter;

/**
 * to compatible with legacy aggregation group in pre-newten
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NAggregationGroup implements Serializable {
    public static class HierarchyMask implements Serializable {
        public BigInteger fullMask = BigInteger.ZERO; // 00000111
        public BigInteger[] allMasks; // 00000100,00000110,00000111
        public BigInteger[] dims; // 00000100,00000010,00000001
    }

    @Getter
    @Setter
    @JsonProperty("includes")
    private Integer[] includes;

    @Getter
    @Setter
    @JsonProperty("select_rule")
    private SelectRule selectRule;

    //computed
    private BigInteger partialCubeFullMask;
    private BigInteger mandatoryColumnMask;
    private List<HierarchyMask> hierarchyMasks;
    private List<BigInteger> joints;//each long is a group
    private BigInteger jointDimsMask;
    private List<BigInteger> normalDims;//each long is a single dim
    private NRuleBasedIndex ruleBasedAggIndex;
    private boolean isMandatoryOnlyValid;
    private HashMap<BigInteger, BigInteger> dim2JointMap;

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

        partialCubeFullMask = BigInteger.ZERO;
        for (Integer dimId : this.includes) {
            int index = ruleBasedAggIndex.getColumnBitIndex(dimId);
            partialCubeFullMask = partialCubeFullMask.setBit(index);
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

            BigInteger joint = BigInteger.ZERO;
            for (int i = 0; i < jointDims.length; i++) {
                int index = ruleBasedAggIndex.getColumnBitIndex(jointDims[i]);
                joint = joint.setBit(index);
            }

            Preconditions.checkState(!joint.equals(BigInteger.ZERO));
            joints.add(joint);
        }

        // todo
        for (BigInteger jt : joints) {
            for (int i = 0; i < jt.bitLength(); i++) {
                if (jt.testBit(i)) {
                    BigInteger dim = BigInteger.valueOf(0);
                    dim2JointMap.put(dim.setBit(i), jt);
                }
            }
        }
    }

    private void buildMandatoryColumnMask() {
        mandatoryColumnMask = BigInteger.ZERO;

        Integer[] mandatory_dims = this.selectRule.mandatoryDims;
        if (mandatory_dims == null || mandatory_dims.length == 0) {
            return;
        }

        for (Integer dim : mandatory_dims) {
            Integer index = ruleBasedAggIndex.getColumnBitIndex(dim);
            mandatoryColumnMask = mandatoryColumnMask.setBit(index);
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

            ArrayList<BigInteger> allMaskList = new ArrayList<>();
            ArrayList<BigInteger> dimList = new ArrayList<>();
            for (int i = 0; i < hierarchy_dims.length; i++) {
                Integer index = ruleBasedAggIndex.getColumnBitIndex(hierarchy_dims[i]);
                BigInteger bit = BigInteger.ZERO.setBit(index);

                // combine joint as logic dim
                if (dim2JointMap.get(bit) != null) {
                    bit = dim2JointMap.get(bit);
                }

                mask.fullMask = mask.fullMask.or(bit);
                allMaskList.add(mask.fullMask);
                dimList.add(bit);
            }

            Preconditions.checkState(allMaskList.size() == dimList.size());
            mask.allMasks = new BigInteger[allMaskList.size()];
            mask.dims = new BigInteger[dimList.size()];

            for (int i = 0; i < allMaskList.size(); i++) {
                mask.allMasks[i] = allMaskList.get(i);
                mask.dims[i] = dimList.get(i);
            }

            this.hierarchyMasks.add(mask);
        }
    }

    private void buildNormalDimsMask() {
        //no joint, no hierarchy, no mandatory
        BigInteger leftover = partialCubeFullMask.andNot(mandatoryColumnMask);
        leftover = leftover.andNot(jointDimsMask);
        for (HierarchyMask hierarchyMask : this.hierarchyMasks) {
            leftover = leftover.andNot(hierarchyMask.fullMask);
        }

        this.normalDims = bits(leftover);
    }

    private List<BigInteger> bits(BigInteger x) {
        List<BigInteger> r = Lists.newArrayList();
        BigInteger l = x;
        while (!l.equals(BigInteger.ZERO)) {
            BigInteger bit = BigInteger.ZERO.setBit(l.getLowestSetBit());
            r.add(bit);
            l = l.xor(bit);
        }
        return r;
    }

    public void buildJointDimsMask() {
        BigInteger ret = BigInteger.ZERO;
        for (BigInteger x : joints) {
            ret = ret.or(x);
        }
        this.jointDimsMask = ret;
    }

    public BigInteger getMandatoryColumnMask() {
        return mandatoryColumnMask;
    }

    public List<HierarchyMask> getHierarchyMasks() {
        return hierarchyMasks;
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
    
    public boolean isOnTree(BigInteger cuboidID) {
        if (cuboidID.compareTo(BigInteger.ZERO) <= 0) {
            return false; //cuboid must be greater than 0
        }
        if (!(cuboidID.andNot(partialCubeFullMask).equals(BigInteger.ZERO))) {
            return false; //a cuboid's parent within agg is at most partialCubeFullMask
        }

        return checkMandatoryColumns(cuboidID) && checkHierarchy(cuboidID) && checkJoint(cuboidID);
    }

    private boolean checkMandatoryColumns(BigInteger cuboidID) {
        if (!(cuboidID.and(mandatoryColumnMask).equals(mandatoryColumnMask))) {
            return false;
        } else {
            //base cuboid is always valid
            if (cuboidID == ruleBasedAggIndex.getFullMask()) {
                return true;
            }

            //cuboid with only mandatory columns maybe valid
            return isMandatoryOnlyValid || !(cuboidID.andNot(mandatoryColumnMask).equals(BigInteger.ZERO));
        }
    }

    private boolean checkJoint(BigInteger cuboidID) {
        for (BigInteger joint : joints) {
            BigInteger common = cuboidID.and(joint);
            if (!(common.equals(BigInteger.ZERO) || common.equals(joint))) {
                return false;
            }
        }
        return true;
    }

    private boolean checkHierarchy(BigInteger cuboidID) {
        // if no hierarchy defined in metadata
        if (hierarchyMasks == null || hierarchyMasks.size() == 0) {
            return true;
        }

        for (HierarchyMask hierarchy : hierarchyMasks) {
            BigInteger result = cuboidID.and(hierarchy.fullMask);
            if (result.compareTo(BigInteger.ZERO) > 0) {
                boolean meetHierarcy = false;
                for (BigInteger mask : hierarchy.allMasks) {
                    if (result.equals(mask)) {
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

    public List<BigInteger> getJoints() {
        return joints;
    }

    public List<BigInteger> getNormalDims() {
        return normalDims;
    }

    public BigInteger getPartialCubeFullMask() {
        return partialCubeFullMask;
    }

    public boolean isMandatoryOnlyValid() {
        return isMandatoryOnlyValid;
    }

    public int getDimCap() {
        return this.selectRule.dimCap == null ? 0 : this.selectRule.dimCap;
    }
}
