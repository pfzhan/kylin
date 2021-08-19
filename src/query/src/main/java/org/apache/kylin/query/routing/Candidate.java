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

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;

import lombok.Getter;
import lombok.Setter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Candidate {

    public static final CandidateComparator COMPARATOR = new CandidateComparator();

    // ============================================================================

    IRealization realization;
    @Getter
    OLAPContext ctx;
    SQLDigest sqlDigest;
    CapabilityResult capability;
    @Getter
    @Setter
    OLAPContextProp rewrittenCtx;
    @Getter
    @Setter
    Map<String, String> aliasMap;

    @Getter
    @Setter
    private List<NDataSegment> prunedSegments;

    @Getter
    @Setter
    private List<NDataSegment> prunedStreamingSegments;

    public void setPrunedSegments(List<NDataSegment> prunedSegments, boolean isStreaming) {
        if (isStreaming) {
            this.prunedStreamingSegments = prunedSegments;
        } else {
            this.prunedSegments = prunedSegments;
        }
    }

    @Getter
    @Setter
    private Map<String, List<Long>> prunedPartitions;

    public Candidate(IRealization realization, OLAPContext ctx, Map<String, String> aliasMap) {
        this.realization = realization;
        this.ctx = ctx;
        this.aliasMap = aliasMap;
    }

    // for testing only
    Candidate() {
    }

    public IRealization getRealization() {
        return realization;
    }

    public CapabilityResult getCapability() {
        return capability;
    }

    public void setCapability(CapabilityResult capability) {
        this.capability = capability;
    }

    @Override
    public String toString() {
        return realization.toString();
    }

    public static class CandidateComparator implements Comparator<Candidate> {

        @Override
        public int compare(Candidate c1, Candidate c2) {
            IRealization real1 = c1.getRealization();
            IRealization real2 = c2.getRealization();

            if (QueryContext.current().getModelPriorities().length > 0) {

                Map<String, Integer> priorities = new HashMap<>();
                for (int i = 0; i < QueryContext.current().getModelPriorities().length; i++) {
                    priorities.put(QueryContext.current().getModelPriorities()[i], i);
                }

                int comp = priorities.getOrDefault(real1.getModel().getAlias().toUpperCase(Locale.ROOT),
                        Integer.MAX_VALUE)
                        - priorities.getOrDefault(real2.getModel().getAlias().toUpperCase(Locale.ROOT),
                                Integer.MAX_VALUE);
                if (comp != 0) {
                    return comp;
                }
            }

            int comp = real1.getCost() - real2.getCost();
            if (comp != 0) {
                return comp;
            }

            comp = Double.compare(c1.capability.cost, c2.capability.cost);
            if (comp != 0) {
                return comp;
            }

            return real1.getModel().getId().compareTo(real2.getModel().getId());
        }
    }
}
