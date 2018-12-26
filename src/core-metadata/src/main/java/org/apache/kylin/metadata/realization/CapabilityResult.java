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

package org.apache.kylin.metadata.realization;

import java.util.Collection;
import java.util.List;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;

public class CapabilityResult {

    /**
     * Is capable or not
     */
    public boolean capable = false;

    /**
     * selected capable candidate, like Lookup or layout
     */
    @Getter
    @Setter
    private IRealizationCandidate selectedCandidate;

    /**
     * The smaller the cost, the more capable the realization
     */
    public int cost;

    /**
     * reason of incapable
     */
    public IncapableCause incapableCause;

    /**
     * Marker objects to indicate all special features
     * (dimension-as-measure, topN etc.) that have influenced the capability check.
     */
    public List<CapabilityInfluence> influences = Lists.newArrayListWithCapacity(1);

    public static interface CapabilityInfluence {
        /**
         * Suggest a multiplier to influence query cost
         */
        double suggestCostMultiplier();

        MeasureDesc getInvolvedMeasure();
    }

    public static class DimensionAsMeasure implements CapabilityInfluence {

        final FunctionDesc function;

        public DimensionAsMeasure(FunctionDesc function) {
            this.function = function;
        }

        @Override
        public double suggestCostMultiplier() {
            return 1;
        }

        @Override
        public MeasureDesc getInvolvedMeasure() {
            return null;
        }

        public FunctionDesc getMeasureFunction() {
            return function;
        }
    }

    public static enum IncapableType {
        UNMATCHED_DIMENSION, UNMATCHED_AGGREGATION, UNSUPPORT_MASSIN, UNSUPPORT_RAWQUERY, LIMIT_PRECEDE_AGGR, II_UNMATCHED_FACT_TABLE, TABLE_INDEX_MISSING_COLS, NOT_EXIST_SNAPSHOT
    }

    public static class IncapableCause {
        private IncapableType incapableType;
        private Collection<TblColRef> unmatchedDimensions;
        private Collection<FunctionDesc> unmatchedAggregations;

        public static IncapableCause unmatchedDimensions(Collection<TblColRef> unmatchedDimensions) {
            IncapableCause incapableCause = new IncapableCause();
            incapableCause.setIncapableType(IncapableType.UNMATCHED_DIMENSION);
            incapableCause.setUnmatchedDimensions(unmatchedDimensions);
            return incapableCause;
        }

        public static IncapableCause unmatchedAggregations(Collection<FunctionDesc> unmatchedAggregations) {
            IncapableCause incapableCause = new IncapableCause();
            incapableCause.setIncapableType(IncapableType.UNMATCHED_AGGREGATION);
            incapableCause.setUnmatchedAggregations(unmatchedAggregations);
            return incapableCause;
        }

        public static IncapableCause create(IncapableType incapableType) {
            IncapableCause incapableCause = new IncapableCause();
            incapableCause.setIncapableType(incapableType);
            return incapableCause;
        }

        public IncapableType getIncapableType() {
            return incapableType;
        }

        public void setIncapableType(IncapableType incapableType) {
            this.incapableType = incapableType;
        }

        public Collection<TblColRef> getUnmatchedDimensions() {
            return unmatchedDimensions;
        }

        public void setUnmatchedDimensions(Collection<TblColRef> unmatchedDimensions) {
            this.unmatchedDimensions = unmatchedDimensions;
        }

        public Collection<FunctionDesc> getUnmatchedAggregations() {
            return unmatchedAggregations;
        }

        public void setUnmatchedAggregations(Collection<FunctionDesc> unmatchedAggregations) {
            this.unmatchedAggregations = unmatchedAggregations;
        }
    }
}
