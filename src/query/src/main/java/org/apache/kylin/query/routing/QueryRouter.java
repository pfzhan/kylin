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

package org.apache.kylin.query.routing;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.CapabilityResult.DimensionAsMeasure;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * @author xjiang
 */
public class QueryRouter {

    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);

    public static IRealization selectRealization(OLAPContext olapContext, Set<IRealization> realizations)
            throws NoRealizationFoundException {
        String factTableName = olapContext.firstTableScan.getTableName();
        String projectName = olapContext.olapSchema.getProjectName();
        SQLDigest sqlDigest = olapContext.getSQLDigest();
        List<Candidate> candidates = Lists.newArrayListWithCapacity(realizations.size());
        for (IRealization real : realizations) {
            if (real.isReady())
                candidates.add(new Candidate(real, sqlDigest));
        }
        logger.info("Find candidates by table " + factTableName + " and project=" + projectName + " : "
                + StringUtils.join(candidates, ","));

        List<Candidate> originCandidates = Lists.newArrayList(candidates);
        // rule based realization selection, rules might reorder realizations or remove specific realization
        RoutingRule.applyRules(candidates);
        collectIncapableReason(olapContext, originCandidates);
        if (candidates.size() == 0) {
            return null;
        }

        Candidate chosen = candidates.get(0);
        adjustForDimensionAsMeasure(chosen, olapContext);
        for (CapabilityInfluence influence : chosen.getCapability().influences) {
            if (influence.getInvolvedMeasure() != null) {
                olapContext.involvedMeasure.add(influence.getInvolvedMeasure());
            }
        }

        logger.info("The realizations remaining: " + RoutingRule.getPrintableText(candidates)
                + ",and the final chosen one for current olap context " + olapContext.id + " is "
                + chosen.realization.getCanonicalName());
        return chosen.realization;
    }

    private static void adjustForDimensionAsMeasure(Candidate chosen, OLAPContext olapContext) {
        CapabilityResult capability = chosen.getCapability();
        for (CapabilityInfluence inf : capability.influences) {
            // convert the metric to dimension
            if (inf instanceof DimensionAsMeasure) {
                FunctionDesc functionDesc = ((DimensionAsMeasure) inf).getMeasureFunction();
                functionDesc.setDimensionAsMetric(true);
                addToContextGroupBy(functionDesc.getParameter().getColRefs(), olapContext);
                logger.info("Adjust DimensionAsMeasure for " + functionDesc);
            }
        }
    }

    private static void addToContextGroupBy(List<TblColRef> colRefs, OLAPContext context) {
        for (TblColRef col : colRefs) {
            if (col.isInnerColumn() == false && context.belongToContextTables(col))
                context.groupByColumns.add(col);
        }
    }

    private static void collectIncapableReason(OLAPContext olapContext, List<Candidate> candidates) {
        //TODO
//        for (Candidate candidate : candidates) {
//            if (!candidate.getCapability().capable) {
//                RealizationCheck.IncapableReason reason = RealizationCheck.IncapableReason
//                        .create(candidate.getCapability().incapableCause);
//                if (reason != null)
//                    olapContext.realizationCheck.addIncapableCube(candidate.getRealization(), reason);
//            } else {
//                olapContext.realizationCheck.addCapableCube(candidate.getRealization());
//            }
//        }
    }
}
