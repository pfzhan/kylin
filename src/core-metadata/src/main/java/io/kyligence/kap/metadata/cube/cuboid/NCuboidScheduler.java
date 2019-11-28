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

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;

/**
 * Defines a cuboid tree, rooted by the base cuboid. A parent cuboid generates its child cuboids.
 */
abstract public class NCuboidScheduler implements Serializable {

    public static NCuboidScheduler getInstance(IndexPlan indexPlan) {
        return new NKECuboidScheduler(indexPlan, null);
    }

    public static NCuboidScheduler getInstance(IndexPlan indexPlan, NRuleBasedIndex ruleBasedCuboidsDes) {
        return new NKECuboidScheduler(indexPlan, ruleBasedCuboidsDes);
    }

    // ============================================================================

    protected final IndexPlan indexPlan;
    protected final NRuleBasedIndex ruleBasedAggIndex;

    protected NCuboidScheduler(final IndexPlan indexPlan, NRuleBasedIndex ruleBasedAggIndex) {
        this.indexPlan = indexPlan;
        this.ruleBasedAggIndex = ruleBasedAggIndex == null ? indexPlan.getRuleBasedIndex() : ruleBasedAggIndex;
    }

    /**
     * Returns all cuboids on the tree.
     */
    abstract public List<BigInteger> getAllCuboidIds();

    /**
     * Returns the number of all cuboids.
     */
    abstract public int getCuboidCount();

    /**
     * optional
     */
    abstract public List<BigInteger> calculateCuboidsForAggGroup(NAggregationGroup agg);

    // ============================================================================

    public IndexPlan getIndexPlan() {
        return indexPlan;
    }
}
