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

package io.kyligence.kap.modeling.smart.proposer;

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.modeling.smart.ModelingContext;

public class ConfigOverrideProposer extends AbstractProposer {
    private static final Logger logger = LoggerFactory.getLogger(ConfigOverrideProposer.class);

    private final static String AGG_MAX_COMBINATION = "kylin.cube.aggrgroup.max-combination";

    public ConfigOverrideProposer(ModelingContext context) {
        super(context);
    }

    @Override
    void doPropose(CubeDesc workCubeDesc) {
        adjustCuboidCombination(workCubeDesc);
    }

    private void adjustCuboidCombination(CubeDesc workCubeDesc) {
        long combinationMax = 0;
        for (AggregationGroup aggGroup : workCubeDesc.getAggregationGroups()) {
            combinationMax = Math.max(combinationMax, aggGroup.calculateCuboidCombination());
        }
        combinationMax = Long.highestOneBit(combinationMax) << 1;

        long defaultMax = KylinConfig.createKylinConfig(new Properties()).getCubeAggrGroupMaxCombination();
        workCubeDesc.getOverrideKylinProps().remove(AGG_MAX_COMBINATION);
        if (combinationMax > defaultMax) {
            logger.warn("Suggested max aggregation group combination exceeds default threshold: combination={}, threshold={}", combinationMax, AGG_MAX_COMBINATION);
            if (context.getModelingConfig().getCuboidCombinationOverride()) {
                workCubeDesc.getOverrideKylinProps().put(AGG_MAX_COMBINATION, Long.toString(combinationMax));
            }
        }
    }
}
