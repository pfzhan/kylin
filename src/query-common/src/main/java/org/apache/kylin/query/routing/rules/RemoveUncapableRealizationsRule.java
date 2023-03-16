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

package org.apache.kylin.query.routing.rules;

import java.util.Iterator;
import java.util.List;

import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.QueryRouter;
import org.apache.kylin.query.routing.RoutingRule;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.apache.kylin.query.util.QueryAliasMatchInfo;

import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;

import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class RemoveUncapableRealizationsRule extends RoutingRule {

    @Override
    public void apply(List<Candidate> candidates) {
        for (Iterator<Candidate> iterator = candidates.iterator(); iterator.hasNext(); ) {
            Candidate candidate = iterator.next();
            if (candidate.getCapability() != null) {
                continue;
            }
            OLAPContext olapContext = candidate.getCtx();
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.putAll(candidate.getAliasMap());
            OLAPContextProp preservedOLAPContext = QueryRouter.preservePropsBeforeRewrite(olapContext);
            ComputedColumnRewriter.rewriteCcInnerCol(olapContext, candidate.getRealization().getModel(),
                    new QueryAliasMatchInfo(aliasMapping, null));
            olapContext.resetSQLDigest();
            CapabilityResult capabilityResult = candidate.getRealization().isCapable(olapContext.getSQLDigest(),
                    candidate.getPrunedSegments(), candidate.getPrunedStreamingSegments(),
                    candidate.getSecondStorageSegmentLayoutMap());
            candidate.setCapability(capabilityResult);

            if (!capabilityResult.capable) {
                QueryRouter.restoreOLAPContextProps(olapContext, preservedOLAPContext);
                capabilityResult = candidate.getRealization().isCapable(candidate.getCtx().getSQLDigest(),
                        candidate.getPrunedSegments(), candidate.getPrunedStreamingSegments(),
                        candidate.getSecondStorageSegmentLayoutMap());
            }
            if (!capabilityResult.capable) {
                iterator.remove();
            }
        }
    }

}
