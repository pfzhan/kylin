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
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.OutOfMaxCombinationException;
import org.apache.kylin.cube.model.SelectRule;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.math.LongMath;

import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import lombok.Getter;
import lombok.Setter;

/**
 * to compatible with legacy aggregation group in pre-newten
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NAggregationGroup implements Serializable {

    public static class HierarchyMask implements Serializable {
        @Getter
        @Setter
        private BigInteger fullMask = BigInteger.ZERO; // 00000111

        @Getter
        @Setter
        private BigInteger[] allMasks; // 00000100,00000110,00000111
    }

    @Getter
    @Setter
    @JsonProperty("includes")
    private Integer[] includes;

    @Getter
    @Setter
    @JsonProperty("measures")
    private Integer[] measures;

    @Getter
    @Setter
    @JsonProperty("select_rule")
    private SelectRule selectRule;

    //computed
    private BigInteger partialCubeFullMask;
    private BigInteger mandatoryColumnMask;
    private BigInteger measureMask;
    private List<HierarchyMask> hierarchyMasks;
    private List<BigInteger> joints;//each long is a group
    private BigInteger jointDimsMask;
    private List<BigInteger> normalDimMeas;//each long is a single dim
    protected RuleBasedIndex ruleBasedAggIndex;
    private boolean isMandatoryOnlyValid;
    private HashMap<BigInteger, BigInteger> dim2JointMap;

    public void init(RuleBasedIndex ruleBasedCuboidsDesc) {
        this.ruleBasedAggIndex = ruleBasedCuboidsDesc;
        this.isMandatoryOnlyValid = ruleBasedCuboidsDesc.getIndexPlan().getConfig()
                .getCubeAggrGroupIsMandatoryOnlyValid();

        if (this.includes == null || this.includes.length == 0 || this.selectRule == null) {
            throw new IllegalStateException("AggregationGroup incomplete");
        }

        checkAndNormalizeFields();

        buildMeasureMask();
        buildPartialCubeFullMask();
        buildMandatoryColumnMask();
        buildJointColumnMask();
        buildJointDimsMask();
        buildHierarchyMasks();
        buildNormalDimsMask();
    }

    private void buildMeasureMask() {
        if (measures == null || measures.length == 0) {
            measures = ruleBasedAggIndex.getMeasures().toArray(new Integer[0]);
        }

        measureMask = buildMask(measures);
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

        partialCubeFullMask = buildMask(includes);
    }

    private BigInteger buildMask(Integer[] ids) {
        BigInteger mask = measureMask == null ? BigInteger.ZERO : measureMask;

        if (ids == null || ids.length == 0)
            return mask;

        for (Integer id : ids) {
            mask = mask.setBit(ruleBasedAggIndex.getColumnBitIndex(id));
        }

        return mask;
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

            BigInteger joint = buildMask(jointDims);

            Preconditions.checkState(!joint.equals(measureMask));
            joints.add(joint);
        }

        for (BigInteger jt : joints) {
            for (int i = 0; i < jt.bitLength(); i++) {
                if (jt.testBit(i)) {
                    BigInteger dim = BigInteger.ZERO;
                    dim = dim.setBit(i).or(measureMask);
                    dim2JointMap.put(dim, jt);
                }
            }
        }
    }

    private void buildMandatoryColumnMask() {
        mandatoryColumnMask = buildMask(this.selectRule.mandatoryDims);
    }

    private void buildHierarchyMasks() {
        this.hierarchyMasks = Lists.newArrayList();

        if (this.selectRule.hierarchyDims == null || this.selectRule.hierarchyDims.length == 0) {
            return;
        }

        for (Integer[] hierarchyDims : this.selectRule.hierarchyDims) {
            HierarchyMask mask = new HierarchyMask();
            if (hierarchyDims == null || hierarchyDims.length == 0) {
                continue;
            }

            ArrayList<BigInteger> allMaskList = new ArrayList<>();
            for (int i = 0; i < hierarchyDims.length; i++) {
                Integer index = ruleBasedAggIndex.getColumnBitIndex(hierarchyDims[i]);
                BigInteger bit = BigInteger.ZERO.setBit(index);
                bit = bit.or(measureMask);

                // combine joint as logic dim
                if (dim2JointMap.get(bit) != null) {
                    bit = dim2JointMap.get(bit);
                }

                mask.fullMask = mask.fullMask.or(bit);
                allMaskList.add(mask.fullMask);
            }

            mask.allMasks = allMaskList.stream().toArray(BigInteger[]::new);
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

        this.normalDimMeas = bits(leftover);
    }

    private List<BigInteger> bits(BigInteger x) {
        List<BigInteger> r = Lists.newArrayList();
        BigInteger l = x;
        while (!l.equals(BigInteger.ZERO)) {
            BigInteger bit = BigInteger.ZERO.setBit(l.getLowestSetBit());
            r.add(bit);
            l = l.xor(bit);
        }

        return r.stream().map(bit -> bit.or(measureMask)).collect(Collectors.toList());
    }

    public void buildJointDimsMask() {
        BigInteger ret = measureMask;
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
            if (this.getDimCap() > 0 || ruleBasedAggIndex.getGlobalDimCap() > 0) {
                try {
                    CuboidScheduler cuboidScheduler = CuboidScheduler.getInstance(ruleBasedAggIndex.getIndexPlan(),
                            ruleBasedAggIndex, true);
                    combination = cuboidScheduler.calculateCuboidsForAggGroup(this).size();
                } catch (OutOfMaxCombinationException oe) {
                    return Long.MAX_VALUE;
                }
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

    public boolean isOnTree(BigInteger cuboidId) {
        if (cuboidId.compareTo(BigInteger.ZERO) <= 0 || cuboidId.compareTo(measureMask) <= 0) {
            return false; //cuboid must be greater than 0
        }
        if (!(cuboidId.andNot(partialCubeFullMask).equals(BigInteger.ZERO))) {
            return false; //a cuboid's parent within agg is at most partialCubeFullMask
        }

        return checkMandatoryColumns(cuboidId) && checkHierarchy(cuboidId) && checkJoint(cuboidId);
    }

    private boolean checkMandatoryColumns(BigInteger cuboidID) {
        if (!(cuboidID.and(mandatoryColumnMask).equals(mandatoryColumnMask))) {
            return false;
        } else {
            //base cuboid is always valid
            boolean baseCuboidValid = ruleBasedAggIndex.getModel().getConfig().isBaseCuboidAlwaysValid();
            if (baseCuboidValid && cuboidID.equals(ruleBasedAggIndex.getFullMask())) {
                return true;
            }

            //cuboid with only mandatory columns maybe valid
            return isMandatoryOnlyValid || !(cuboidID.andNot(mandatoryColumnMask).equals(BigInteger.ZERO));
        }
    }

    private boolean checkJoint(BigInteger cuboidID) {
        for (BigInteger joint : joints) {
            BigInteger common = cuboidID.and(joint);
            if (!(common.equals(measureMask) || common.equals(joint))) {
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
            if (result.compareTo(measureMask) > 0) {
                boolean meetHierarchy = false;
                for (BigInteger mask : hierarchy.allMasks) {
                    if (result.equals(mask)) {
                        meetHierarchy = true;
                        break;
                    }
                }

                if (!meetHierarchy) {
                    return false;
                }
            }
        }
        return true;
    }

    public List<BigInteger> getJoints() {
        return joints;
    }

    public List<BigInteger> getNormalDimMeas() {
        return normalDimMeas;
    }

    public BigInteger getPartialCubeFullMask() {
        return partialCubeFullMask;
    }

    public BigInteger getMeasureMask() {
        return measureMask;
    }

    public boolean isMandatoryOnlyValid() {
        return isMandatoryOnlyValid;
    }

    public int getDimCap() {
        return this.selectRule.dimCap == null ? 0 : this.selectRule.dimCap;
    }


    boolean checkDimCap(BigInteger cuboidID) {
        int dimCap = getDimCap();

        if (dimCap == 0)
            dimCap = ruleBasedAggIndex.getGlobalDimCap();

        if (dimCap <= 0)
            return true;

        int dimCount = 0;

        for (BigInteger normal : getNormalDimMeas()) {
            if (!(cuboidID.and(normal)).equals(getMeasureMask())) {
                dimCount++;
            }
        }

        for (BigInteger joint : getJoints()) {
            if (!(cuboidID.and(joint)).equals(getMeasureMask()))
                dimCount++;
        }

        for (HierarchyMask hierarchy : getHierarchyMasks()) {
            if (!(cuboidID.and(hierarchy.getFullMask())).equals(getMeasureMask()))
                dimCount++;
        }

        return dimCount <= dimCap;
    }
}
