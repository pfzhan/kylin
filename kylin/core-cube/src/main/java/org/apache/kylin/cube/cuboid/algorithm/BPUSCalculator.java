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

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.


 */

package org.apache.kylin.cube.cuboid.algorithm;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Calculate the benefit based on Benefit Per Unit Space.
 */
public class BPUSCalculator implements BenefitPolicy {

    private static Logger logger = LoggerFactory.getLogger(BPUSCalculator.class);

    protected CuboidStats cuboidStats;
    protected Map<Long, Long> cuboidAggCostMap;

    public BPUSCalculator(CuboidStats cuboidStats) {
        this.cuboidStats = cuboidStats;
        this.cuboidAggCostMap = Maps.newHashMap();
    }

    @Override
    public void initBeforeStart() {
        cuboidAggCostMap.clear();
        //Initialize stats for mandatory cuboids
        for (Long cuboid : cuboidStats.getAllCuboidsForMandatory()) {
            if (getCuboidCost(cuboid) != null) {
                cuboidAggCostMap.put(cuboid, getCuboidCost(cuboid));
            }
        }
        Set<Long> mandatoryCuboidSetWithStats = cuboidAggCostMap.keySet();
        //Initialize stats for selection cuboids
        long baseCuboidCost = getCuboidCost(cuboidStats.getBaseCuboid());
        for (Long cuboid : cuboidStats.getAllCuboidsForSelection()) {
            long leastCost = baseCuboidCost;
            for (Long cuboidTarget : mandatoryCuboidSetWithStats) {
                if ((cuboid | cuboidTarget) == cuboidTarget) {
                    if (leastCost > cuboidAggCostMap.get(cuboidTarget)) {
                        leastCost = cuboidAggCostMap.get(cuboidTarget);
                    }
                }
            }
            cuboidAggCostMap.put(cuboid, leastCost);
        }
    }

    @Override
    public CuboidBenefitModel.BenefitModel calculateBenefit(long cuboid, Set<Long> selected) {
        double totalCostSaving = 0;
        int benefitCount = 0;
        for (Long descendant : cuboidStats.getAllDescendants(cuboid)) {
            if (!selected.contains(descendant)) {
                double costSaving = getCostSaving(descendant, cuboid);
                if (costSaving > 0) {
                    totalCostSaving += costSaving;
                    benefitCount++;
                }
            }
        }

        double spaceCost = calculateSpaceCost(cuboid);
        double benefitPerUnitSpace = totalCostSaving / spaceCost;
        return new CuboidBenefitModel.BenefitModel(benefitPerUnitSpace, benefitCount);
    }

    @Override
    public CuboidBenefitModel.BenefitModel calculateBenefitTotal(List<Long> cuboidsToAdd, Set<Long> selected) {
        Set<Long> selectedInner = Sets.newHashSet(selected);
        Map<Long, Long> cuboidAggCostMapSnapshot = Maps.newHashMap(cuboidAggCostMap);
        for (Long cuboid : cuboidsToAdd) {
            selectedInner.add(cuboid);
            propagateAggregationCost(cuboid, selectedInner);
        }
        double totalCostSaving = 0;
        int benefitCount = 0;
        for (Long cuboid : cuboidAggCostMap.keySet()) {
            if (cuboidAggCostMap.get(cuboid) < cuboidAggCostMapSnapshot.get(cuboid)) {
                totalCostSaving += cuboidAggCostMapSnapshot.get(cuboid) - cuboidAggCostMap.get(cuboid);
                benefitCount++;
            }
        }
        cuboidAggCostMap = cuboidAggCostMapSnapshot;

        double benefitPerUnitSpace = totalCostSaving;
        return new CuboidBenefitModel.BenefitModel(benefitPerUnitSpace, benefitCount);
    }

    protected double getCostSaving(long descendant, long cuboid) {
        long cuboidCost = getCuboidCost(cuboid);
        long descendantAggCost = getCuboidAggregationCost(descendant);
        return descendantAggCost - cuboidCost;
    }

    protected Long getCuboidCost(long cuboid) {
        return cuboidStats.getCuboidCount(cuboid);
    }

    private long getCuboidAggregationCost(long cuboid) {
        return cuboidAggCostMap.get(cuboid);
    }

    @Override
    public boolean ifEfficient(CuboidBenefitModel best) {
        if (best.getBenefit() < getMinBenefitRatio()) {
            logger.info(String.format("The recommended cuboid %s doesn't meet minimum benifit ratio %f", best,
                    getMinBenefitRatio()));
            return false;
        }
        return true;
    }

    public double getMinBenefitRatio() {
        return 0.01;
    }

    @Override
    public void propagateAggregationCost(long cuboid, Set<Long> selected) {
        long aggregationCost = getCuboidCost(cuboid);
        Set<Long> childrenCuboids = cuboidStats.getAllDescendants(cuboid);
        for (Long child : childrenCuboids) {
            if (!selected.contains(child) && (aggregationCost < getCuboidAggregationCost(child))) {
                cuboidAggCostMap.put(child, aggregationCost);
            }
        }
    }

    /**
     * Return the space cost of building a cuboid.
     *
     */
    public double calculateSpaceCost(long cuboid) {
        return cuboidStats.getCuboidCount(cuboid);
    }

    @Override
    public BenefitPolicy getInstance() {
        BPUSCalculator bpusCalculator = new BPUSCalculator(this.cuboidStats);
        bpusCalculator.cuboidAggCostMap.putAll(this.cuboidAggCostMap);
        return bpusCalculator;
    }
}