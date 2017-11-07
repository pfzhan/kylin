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

import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.util.CubeDescUtil;

public class QueryAggrGroupProposer extends AbstractCubeProposer {
    private static final Logger logger = LoggerFactory.getLogger(QueryAggrGroupProposer.class);

    public QueryAggrGroupProposer(CubeContext context) {
        super(context);
    }

    @Override
    void doPropose(CubeDesc workCubeDesc) {
        if (!context.hasQueryStats()) {
            logger.debug("No query stats found, skip proposing aggregation groups.");
            return;
        }

        DataModelDesc modelDesc = workCubeDesc.getModel();
        Set<Set<String>> includeCandidates = Sets.newHashSet();
        for (Set<String> cuboidCols : context.getQueryStats().getCuboids()) {
            Set<String> includeCandidate = Sets.newHashSet();
            for (String cuboidCol : cuboidCols) {
                TblColRef cuboidColRef = modelDesc.findColumn(cuboidCol);
                if (!workCubeDesc.listDimensionColumnsIncludingDerived().contains(cuboidColRef)) {
                    continue;
                }

                if (workCubeDesc.isDerived(cuboidColRef)) {
                    TblColRef[] hostRefs = workCubeDesc.getHostInfo(cuboidColRef).columns;
                    for (TblColRef hostRef : hostRefs) {
                        includeCandidate.add(hostRef.getIdentity());
                    }
                } else {
                    includeCandidate.add(cuboidCol);
                }
            }

            if (!includeCandidate.isEmpty())
                includeCandidates.add(includeCandidate);
        }

        List<AggregationGroup> aggGroups = Lists.newArrayList();
        for (Set<String> includeCols : includeCandidates) {
            String[] includes = includeCols.toArray(new String[0]);
            SelectRule rule = new SelectRule();
            rule.mandatoryDims = includes;

            AggregationGroup aggGroup = new AggregationGroup();
            aggGroup.setIncludes(includes);
            aggGroup.setSelectRule(rule);
            aggGroups.add(aggGroup);
        }
        workCubeDesc.setAggregationGroups(aggGroups);

        CubeDescUtil.fillAggregationGroupsForMPCube(workCubeDesc);
    }
}
