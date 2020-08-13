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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.NotImplementedException;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

/**
 * Defines a cuboid tree, rooted by the base cuboid. A parent cuboid generates its child cuboids.
 */
public abstract class CuboidScheduler implements Serializable {

    protected static final String OUT_OF_MAX_COMBINATION_MSG_FORMAT = "Too many cuboids for the cube. Cuboid combination reached %s and limit is %s. Abort calculation.";

    public static CuboidScheduler getInstance(IndexPlan indexPlan, NRuleBasedIndex ruleBasedIndex, boolean skipAll) {
        if (ruleBasedIndex.getSchedulerVersion() == 1) {
            return new KECuboidSchedulerV1(indexPlan, ruleBasedIndex, skipAll);
        } else if (ruleBasedIndex.getSchedulerVersion() == 2) {
            return new KECuboidSchedulerV2(indexPlan, ruleBasedIndex, skipAll);
        }
        throw new NotImplementedException("Not Support version " + ruleBasedIndex.getSchedulerVersion());
    }

    public static CuboidScheduler getInstance(IndexPlan indexPlan, NRuleBasedIndex ruleBasedIndex) {
        return getInstance(indexPlan, ruleBasedIndex, false);
    }

    // ============================================================================

    protected final IndexPlan indexPlan;
    protected final NRuleBasedIndex ruleBasedAggIndex;

    protected CuboidScheduler(final IndexPlan indexPlan, NRuleBasedIndex ruleBasedAggIndex) {
        this.indexPlan = indexPlan;
        this.ruleBasedAggIndex = ruleBasedAggIndex == null ? indexPlan.getRuleBasedIndex() : ruleBasedAggIndex;
    }

    /**
     * Returns all cuboids on the tree.
     */
    public abstract List<ColOrder> getAllColOrders();

    /**
     * Returns the number of all cuboids.
     */
    public abstract int getCuboidCount();

    public abstract void validateOrder();

    public abstract void updateOrder();

    /**
     * optional
     */
    public abstract List<ColOrder> calculateCuboidsForAggGroup(NAggregationGroup agg);

    // ============================================================================

    public IndexPlan getIndexPlan() {
        return indexPlan;
    }

    protected ColOrder extractDimAndMeaFromBigInt(BigInteger bigInteger) {
        val allDims = ruleBasedAggIndex.getDimensions();
        val allMeas = ruleBasedAggIndex.getMeasures();
        return extractDimAndMeaFromBigInt(allDims, allMeas, bigInteger);
    }

    protected ColOrder extractDimAndMeaFromBigInt(List<Integer> allDims, List<Integer> allMeas, BigInteger bigInteger) {
        val dims = Lists.<Integer> newArrayList();
        val meas = Lists.<Integer> newArrayList();

        int size = allDims.size() + allMeas.size();

        for (int i = 0; i < size; i++) {
            int shift = size - i - 1;
            if (bigInteger.testBit(shift)) {
                if (i >= allDims.size()) {
                    meas.add(allMeas.get(i - allDims.size()));
                } else {
                    dims.add(allDims.get(i));
                }
            }
        }

        return new ColOrder(dims, meas);
    }

    @Data
    @AllArgsConstructor
    public static class ColOrder {
        private List<Integer> dimensions;
        private List<Integer> measures;

        public List<Integer> toList() {
            return Stream.concat(dimensions.stream(), measures.stream()).collect(Collectors.toList());
        }

    }
}
