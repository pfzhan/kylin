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

package org.apache.kylin.metadata.filter;

import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * optimize the filter if possible, not limited to:
 * 
 * 1. replace filters like (a = 10 OR 1 = 1) to ConstantTupleFilter.TRUE
 * 2. replace filters like (a = 10 AND 1 = 2) to ConstantTupleFilter.FALSE
 * 
 * 3. replace filter like (a = 10 AND ConstantTupleFilter.TRUE) to (a = 10)
 * 4. replace filter like (a = 10 OR ConstantTupleFilter.FALSE) to (a = 10)
 * 
 * is a first type transformer defined in ITupleFilterTransformer
 */
public class FilterOptimizeTransformer implements ITupleFilterTransformer {
    public static final Logger logger = LoggerFactory.getLogger(FilterOptimizeTransformer.class);

    @Override
    public TupleFilter transform(TupleFilter tupleFilter) {
        if (tupleFilter == null || !(tupleFilter instanceof IOptimizeableTupleFilter))
            return tupleFilter;
        else
            return ((IOptimizeableTupleFilter) tupleFilter).acceptOptimizeTransformer(this);
    }

    public TupleFilter visit(CompareTupleFilter compareTupleFilter) {
        if (compareTupleFilter != null) {
            CompareTupleFilter.CompareResultType compareResultType = compareTupleFilter.getCompareResultType();

            if (compareResultType == CompareTupleFilter.CompareResultType.AlwaysTrue) {
                logger.debug("Optimize CompareTupleFilter {{}} to ConstantTupleFilter.TRUE", compareTupleFilter);
                return ConstantTupleFilter.TRUE;
            } else if (compareResultType == CompareTupleFilter.CompareResultType.AlwaysFalse) {
                logger.debug("Optimize CompareTupleFilter {{}} to ConstantTupleFilter.FALSE", compareTupleFilter);
                return ConstantTupleFilter.FALSE;
            }
        }

        return compareTupleFilter;
    }

    public TupleFilter visit(LogicalTupleFilter logicalTupleFilter) {
        if (logicalTupleFilter == null) {
            return null;
        }

        if (logicalTupleFilter.getOperator() == TupleFilter.FilterOperatorEnum.OR) {
            @SuppressWarnings("unchecked")
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) logicalTupleFilter.getChildren()
                    .listIterator();
            while (childIterator.hasNext()) {
                TupleFilter next = childIterator.next();
                if (ConstantTupleFilter.TRUE.equals(next)) {
                    logger.debug("Optimized {{}} to ConstantTupleFilter.TRUE", logicalTupleFilter);
                    return ConstantTupleFilter.TRUE;
                }

                if (ConstantTupleFilter.FALSE.equals(next)) {
                    childIterator.remove();
                }
            }

            if (logicalTupleFilter.getChildren().size() == 0) {
                return ConstantTupleFilter.FALSE;
            }
        } else if (logicalTupleFilter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
            @SuppressWarnings("unchecked")
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) logicalTupleFilter.getChildren()
                    .listIterator();
            while (childIterator.hasNext()) {
                TupleFilter next = childIterator.next();
                if (ConstantTupleFilter.FALSE.equals(next)) {
                    logger.debug("Optimized {{}} to ConstantTupleFilter.FALSE", logicalTupleFilter);
                    return ConstantTupleFilter.FALSE;
                }

                if (ConstantTupleFilter.TRUE.equals(next)) {
                    childIterator.remove();
                }
            }

            if (logicalTupleFilter.getChildren().size() == 0) {
                return ConstantTupleFilter.TRUE;
            }
        }

        return logicalTupleFilter;
    }

    public TupleFilter visit(CaseTupleFilter caseTupleFilter) {

        List<TupleFilter> whenFilters = caseTupleFilter.getWhenFilters();
        List<TupleFilter> thenFilters = caseTupleFilter.getThenFilters();
        List<TupleFilter> newFilters = Lists.newArrayList();
        boolean changed = false;
        for (int i = 0; i < whenFilters.size(); i++) {
            if (whenFilters.get(i) == ConstantTupleFilter.TRUE) {
                return thenFilters.get(i);
            }

            if (whenFilters.get(i) == ConstantTupleFilter.FALSE) {
                changed = true;
                continue;
            }

            newFilters.add(whenFilters.get(i));
            newFilters.add(thenFilters.get(i));
        }
        newFilters.add(caseTupleFilter.getElseFilter());

        if (!changed) {
            return caseTupleFilter;
        } else {
            if (newFilters.size() == 1) {
                return newFilters.get(0);
            }

            CaseTupleFilter newCaseTupleFilter = new CaseTupleFilter();
            newCaseTupleFilter.addChildren(newFilters);
            return newCaseTupleFilter;
        }
    }
}
