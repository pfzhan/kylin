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

package io.kyligence.kap.cube.cuboid;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;

/**
 * Defines a cuboid tree, rooted by the base cuboid. A parent cuboid generates its child cuboids.
 */
abstract public class NCuboidScheduler implements Serializable {

    public static NCuboidScheduler getInstance(NCubePlan nCubePlan) {
        return new NKapCuboidScheduler243(nCubePlan);
    }

    // ============================================================================

    final protected NCubePlan nCubePlan;

    protected NCuboidScheduler(NCubePlan nCubePlan) {
        this.nCubePlan = nCubePlan;
    }

    /** Returns all cuboids on the tree. */
    abstract public Set<Long> getAllCuboidIds();

    /** Returns the number of all cuboids. */
    abstract public int getCuboidCount();

    /** Returns the child cuboids of a parent. */
    abstract public List<Long> getSpanningCuboid(long parentCuboid);

    /** Returns a valid cuboid that best matches the request cuboid. */
    abstract public long findBestMatchCuboid(long requestCuboid);

    /** optional */
    abstract public Set<Long> calculateCuboidsForAggGroup(NAggregationGroup agg);

    // ============================================================================

    private transient List<List<Long>> cuboidsByLayer;

    public long getBaseCuboidId() {
        return nCubePlan.getnRuleBasedCuboidsDesc().getFullMask();
    }

    public NCubePlan getnCubePlan() {
        return nCubePlan;
    }

    /** Checks whether a cuboid is valid or not. */
    public boolean isValid(long requestCuboid) {
        return getAllCuboidIds().contains(requestCuboid);
    }

    /**
     * Get cuboids by layer. It's built from pre-expanding tree.
     * @return layered cuboids
     */
    public List<List<Long>> getCuboidsByLayer() {
        if (cuboidsByLayer != null) {
            return cuboidsByLayer;
        }

        int totalNum = 0;
        cuboidsByLayer = Lists.newArrayList();

        cuboidsByLayer.add(Collections.singletonList(nCubePlan.getnRuleBasedCuboidsDesc().getFullMask()));
        totalNum++;

        List<Long> lastLayer = cuboidsByLayer.get(cuboidsByLayer.size() - 1);
        while (!lastLayer.isEmpty()) {
            List<Long> newLayer = Lists.newArrayList();
            for (Long parent : lastLayer) {
                newLayer.addAll(getSpanningCuboid(parent));
            }
            if (newLayer.isEmpty()) {
                break;
            }
            cuboidsByLayer.add(newLayer);
            totalNum += newLayer.size();
            lastLayer = newLayer;
        }

        int size = getAllCuboidIds().size();
        Preconditions.checkState(totalNum == size, "total Num: " + totalNum + " actual size: " + size);
        return cuboidsByLayer;
    }

    /**
     * Get cuboid level count except base cuboid
     * @return
     */
    public int getBuildLevel() {
        return getCuboidsByLayer().size() - 1;
    }

    /** Returns the key for what this cuboid scheduler responsible for. */
    public String getCuboidCacheKey() {
        return nCubePlan.getName();
    }

}
