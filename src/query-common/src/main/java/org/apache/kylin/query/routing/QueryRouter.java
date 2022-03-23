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
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author xjiang
 */
public class QueryRouter {

    private QueryRouter() {
    }

    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);

    public static Candidate selectRealization(OLAPContext olapContext, IRealization realization,
            Map<String, String> aliasMap) {
        if (!realization.isReady()) {
            logger.warn("Realization {} is not ready", realization);
            return null;
        }

        List<Candidate> candidates = Lists.newArrayListWithCapacity(1);
        candidates.add(new Candidate(realization, olapContext, aliasMap));
        logger.info("Find candidates by table {} and project={} : {}", olapContext.firstTableScan.getTableName(),
                olapContext.olapSchema.getProjectName(), StringUtils.join(candidates, ","));
        List<Candidate> originCandidates = Lists.newArrayList(candidates);

        // rule based realization selection, rules might reorder realizations or remove specific realization
        RoutingRule.applyRules(candidates);

        collectIncapableReason(olapContext, originCandidates);
        if (candidates.isEmpty()) {
            return null;
        }

        Candidate chosen = candidates.get(0);
        chosen.setRewrittenCtx(preserveRewriteProps(olapContext));
        logger.info("The realizations remaining: {}, and the final chosen one for current olap context {} is {}",
                RoutingRule.getPrintableText(candidates), olapContext.id, chosen.realization.getCanonicalName());
        return chosen;
    }

    static OLAPContextProp preserveRewriteProps(OLAPContext rewrittenOLAContext) {
        return preservePropsBeforeRewrite(rewrittenOLAContext);
    }

    static OLAPContextProp preservePropsBeforeRewrite(OLAPContext oriOLAPContext) {
        OLAPContextProp preserved = new OLAPContextProp(-1);
        preserved.allColumns = Sets.newHashSet(oriOLAPContext.allColumns);
        preserved.setSortColumns(Lists.newArrayList(oriOLAPContext.getSortColumns()));
        preserved.setInnerGroupByColumns(Sets.newHashSet(oriOLAPContext.getInnerGroupByColumns()));
        preserved.setGroupByColumns(Sets.newLinkedHashSet(oriOLAPContext.getGroupByColumns()));
        preserved.setInnerFilterColumns(Sets.newHashSet(oriOLAPContext.getInnerFilterColumns()));
        for (FunctionDesc agg : oriOLAPContext.aggregations) {
            preserved.getReservedMap().put(agg,
                    FunctionDesc.newInstance(agg.getExpression(), agg.getParameters(), agg.getReturnType()));
        }

        return preserved;
    }

    static void restoreOLAPContextProps(OLAPContext oriOLAPContext, OLAPContextProp preservedOLAPContext) {
        oriOLAPContext.allColumns = preservedOLAPContext.allColumns;
        oriOLAPContext.setSortColumns(preservedOLAPContext.getSortColumns());
        oriOLAPContext.aggregations.forEach(agg -> {
            if (preservedOLAPContext.getReservedMap().containsKey(agg)) {
                final FunctionDesc functionDesc = preservedOLAPContext.getReservedMap().get(agg);
                agg.setExpression(functionDesc.getExpression());
                agg.setParameters(functionDesc.getParameters());
                agg.setReturnType(functionDesc.getReturnType());
            }
        });
        oriOLAPContext.setGroupByColumns(preservedOLAPContext.getGroupByColumns());
        oriOLAPContext.setInnerGroupByColumns(preservedOLAPContext.getInnerGroupByColumns());
        oriOLAPContext.setInnerFilterColumns(preservedOLAPContext.getInnerFilterColumns());
        oriOLAPContext.resetSQLDigest();
    }

    private static void collectIncapableReason(OLAPContext olapContext, List<Candidate> candidates) {
        for (Candidate candidate : candidates) {
            if (!candidate.getCapability().capable) {
                RealizationCheck.IncapableReason reason = RealizationCheck.IncapableReason
                        .create(candidate.getCapability().incapableCause);
                if (reason != null)
                    olapContext.realizationCheck.addIncapableCube(candidate.getRealization(), reason);
            } else {
                olapContext.realizationCheck.addCapableCube(candidate.getRealization());
            }
        }
    }
}
