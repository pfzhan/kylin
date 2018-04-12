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

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TimeConditionLiteralsReplacer implements TupleFilterSerializer.Decorator {

    private IdentityHashMap<TupleFilter, DataType> dateCompareTupleChildren;

    public TimeConditionLiteralsReplacer(TupleFilter root) {
        this.dateCompareTupleChildren = Maps.newIdentityHashMap();
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter cfilter = (CompareTupleFilter) filter;
            List<? extends TupleFilter> children = cfilter.getChildren();

            if (children == null || children.size() < 1) {
                throw new IllegalArgumentException("Illegal compare filter: " + cfilter);
            }

            TblColRef col = cfilter.getColumn();
            if (col == null || !col.getType().isDateTimeFamily()) {
                return cfilter;
            }

            for (TupleFilter child : filter.getChildren()) {
                dateCompareTupleChildren.put(child, col.getType());
            }
        }

        if (filter instanceof ConstantTupleFilter && dateCompareTupleChildren.containsKey(filter)) {
            ConstantTupleFilter constantTupleFilter = (ConstantTupleFilter) filter;
            Set<String> newValues = Sets.newHashSet();
            DataType columnType = dateCompareTupleChildren.get(filter);

            for (String value : (Collection<String>) constantTupleFilter.getValues()) {
                newValues.add(formatTime(value, columnType));
            }
            return new ConstantTupleFilter(newValues);
        }
        return filter;
    }

    private String formatTime(String dateStr, DataType dataType) {
        if (dataType.isDatetime() || dataType.isTime()) {
            throw new RuntimeException("Datetime and time type are not supported yet");
        }

        if (DateFormat.isSupportedDateFormat(dateStr)) {
            return dateStr;
        }

        long millis = Long.valueOf(dateStr);
        if (dataType.isTimestamp()) {
            return DateFormat.formatToTimeStr(millis);
        } else if (dataType.isDate()) {
            return DateFormat.formatToDateStr(millis);
        } else {
            throw new RuntimeException("Unknown type " + dataType + " to formatTime");
        }
    }
}
