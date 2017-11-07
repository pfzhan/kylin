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

package io.kyligence.kap.smart.cube.proposer;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.cube.CubeContext;

public class MixedAggrGroupProposer extends AbstractCubeProposer {
    private static final Logger logger = LoggerFactory.getLogger(MixedAggrGroupProposer.class);

    private ModelAggrGroupProposer physicalProposer;
    private QueryAggrGroupProposer businessProposer;

    public MixedAggrGroupProposer(CubeContext context) {
        super(context);

        physicalProposer = new ModelAggrGroupProposer(context);
        businessProposer = new QueryAggrGroupProposer(context);
    }

    @Override
    void doPropose(CubeDesc workCubeDesc) {
        List<AggregationGroup> aggGroups = Lists.newArrayList();
        Set<List<String>> whiteLists = Sets.newHashSet();

        // save orig CubeDesc for business proposal
        CubeDesc copyCubeDesc = CubeDesc.getCopyOf(workCubeDesc);
        copyCubeDesc.setAggregationGroups(Lists.<AggregationGroup> newArrayList());

        // 1. get physical proposal
        physicalProposer.propose(workCubeDesc);
        aggGroups.addAll(workCubeDesc.getAggregationGroups());
        logger.debug("Physical AggGroup Proposal: num={}", workCubeDesc.getAggregationGroups().size());

        for (AggregationGroup aggGroup : aggGroups) {
            if (aggGroup.getIncludes().length == aggGroup.getSelectRule().mandatoryDims.length) {
                whiteLists.add(Arrays.asList(aggGroup.getIncludes()));
            }
        }

        // 2. append business proposal to tail
        CubeDesc businessCubeDesc = businessProposer.propose(copyCubeDesc);
        int count = 0;
        if (!businessCubeDesc.getAggregationGroups().isEmpty()) {
            List<AggregationGroup> businessAggGroups = businessCubeDesc.getAggregationGroups();
            for (AggregationGroup businessAggGroup : businessAggGroups) {
                if (!whiteLists.contains(Arrays.asList(businessAggGroup.getIncludes()))) {
                    aggGroups.add(businessAggGroup);
                    count++;
                }
            }
        }
        logger.debug("Business AggGroup Proposal: num={}", count);

        workCubeDesc.setAggregationGroups(aggGroups);
    }
}
