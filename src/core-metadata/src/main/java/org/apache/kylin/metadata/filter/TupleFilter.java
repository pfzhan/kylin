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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import lombok.Getter;

/**
 * 
 * @author xjiang
 * 
 */
public abstract class TupleFilter {

    static final Logger logger = LoggerFactory.getLogger(TupleFilter.class);

    public enum FilterOperatorEnum {
        EQ(1), // equal

        IN(2), ISNULL(3), // inferior equal

        NEQ(4), GT(5), LT(6), GTE(7), LTE(8), NOTIN(9), ISNOTNULL(10), // range

        AND(20), OR(21), NOT(22), // logic op

        COLUMN(30), CONSTANT(31), DYNAMIC(32), EXTRACT(33), CASE(34), //
        FUNCTION(35), MASSIN(36), EVAL_FUNC(37), UNSUPPORTED(38);

        private final int value;

        FilterOperatorEnum(int v) {
            this.value = v;
        }

        public int getValue() {
            return this.value;
        }
    }

    static final Map<FilterOperatorEnum, FilterOperatorEnum> REVERSE_OP_MAP = Maps.newHashMap();
    static final Map<FilterOperatorEnum, FilterOperatorEnum> SWAP_OP_MAP = Maps.newHashMap();

    static {
        REVERSE_OP_MAP.put(FilterOperatorEnum.EQ, FilterOperatorEnum.NEQ);
        REVERSE_OP_MAP.put(FilterOperatorEnum.NEQ, FilterOperatorEnum.EQ);
        REVERSE_OP_MAP.put(FilterOperatorEnum.GT, FilterOperatorEnum.LTE);
        REVERSE_OP_MAP.put(FilterOperatorEnum.LTE, FilterOperatorEnum.GT);
        REVERSE_OP_MAP.put(FilterOperatorEnum.LT, FilterOperatorEnum.GTE);
        REVERSE_OP_MAP.put(FilterOperatorEnum.GTE, FilterOperatorEnum.LT);
        REVERSE_OP_MAP.put(FilterOperatorEnum.IN, FilterOperatorEnum.NOTIN);
        REVERSE_OP_MAP.put(FilterOperatorEnum.NOTIN, FilterOperatorEnum.IN);
        REVERSE_OP_MAP.put(FilterOperatorEnum.ISNULL, FilterOperatorEnum.ISNOTNULL);
        REVERSE_OP_MAP.put(FilterOperatorEnum.ISNOTNULL, FilterOperatorEnum.ISNULL);
        REVERSE_OP_MAP.put(FilterOperatorEnum.AND, FilterOperatorEnum.OR);
        REVERSE_OP_MAP.put(FilterOperatorEnum.OR, FilterOperatorEnum.AND);

        SWAP_OP_MAP.put(FilterOperatorEnum.EQ, FilterOperatorEnum.EQ);
        SWAP_OP_MAP.put(FilterOperatorEnum.NEQ, FilterOperatorEnum.NEQ);
        SWAP_OP_MAP.put(FilterOperatorEnum.GT, FilterOperatorEnum.LT);
        SWAP_OP_MAP.put(FilterOperatorEnum.LTE, FilterOperatorEnum.GTE);
        SWAP_OP_MAP.put(FilterOperatorEnum.LT, FilterOperatorEnum.GT);
        SWAP_OP_MAP.put(FilterOperatorEnum.GTE, FilterOperatorEnum.LTE);
    }

    protected final List<TupleFilter> children;
    protected FilterOperatorEnum operator;

    @Getter
    private TupleFilter parent;

    protected TupleFilter(List<TupleFilter> filters, FilterOperatorEnum op) {
        this.children = filters;
        this.operator = op;
        this.parent = null;
    }

    public void addChild(TupleFilter child) {
        child.parent = this;
        children.add(child);
    }

    public final void addChildren(List<? extends TupleFilter> children) {
        for (TupleFilter c : children)
            addChild(c); // subclass overrides addChild()
    }

    public List<? extends TupleFilter> getChildren() {
        return children;
    }

    public boolean hasChildren() {
        return children != null && !children.isEmpty();
    }

    public FilterOperatorEnum getOperator() {
        return operator;
    }

    public TupleFilter copy() {
        throw new UnsupportedOperationException();
    }

    public TupleFilter reverse() {
        logger.warn("Cannot reverse {}, loosen the filter to true", this);
        return ConstantTupleFilter.TRUE;
    }

    public abstract boolean isEvaluable();

    public abstract boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs);

    public abstract Collection<?> getValues();

    public abstract void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    public abstract void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    public static boolean isEvaluableRecursively(TupleFilter filter) {
        if (filter == null)
            return true;

        if (!filter.isEvaluable())
            return false;

        for (TupleFilter child : filter.getChildren()) {
            if (!isEvaluableRecursively(child))
                return false;
        }
        return true;
    }

    public static void collectColumns(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null || collector == null) {
            return;
        }

        if (filter instanceof ColumnTupleFilter) {
            TblColRef filterCol = ((ColumnTupleFilter) filter).getColumn();
            collector.add(filterCol);

            TblColRef.FilterColEnum tmpLevel;
            final TupleFilter parent = filter.getParent();
            final int parentOpLevel = parent.operator.getValue();
            if (parentOpLevel == FilterOperatorEnum.EQ.getValue()) {
                tmpLevel = TblColRef.FilterColEnum.EQUAL_FILTER;
            } else if (parentOpLevel <= FilterOperatorEnum.ISNULL.getValue()) {
                tmpLevel = TblColRef.FilterColEnum.INFERIOR_EQUAL_FILTER;
            } else if (parentOpLevel <= FilterOperatorEnum.ISNOTNULL.getValue()) {
                tmpLevel = TblColRef.FilterColEnum.RANGE_FILTER;
            } else if (filter.getParent() instanceof BuiltInFunctionTupleFilter
                    && ((BuiltInFunctionTupleFilter) parent).getName().equalsIgnoreCase("LIKE")) {
                tmpLevel = TblColRef.FilterColEnum.LIKE_FILTER;
            } else {
                tmpLevel = TblColRef.FilterColEnum.OTHER_FILTER;
            }

            // update when priority higher than its existing value
            if (tmpLevel.getPriority() > filterCol.getFilterLevel().getPriority()) {
                filterCol.setFilterLevel(tmpLevel);
            }
        }

        for (TupleFilter child : filter.getChildren()) {
            collectColumns(child, collector);
        }
    }

    public static TupleFilter and(TupleFilter f1, TupleFilter f2) {
        if (f1 == null)
            return f2;
        if (f2 == null)
            return f1;

        if (f1.getOperator() == FilterOperatorEnum.AND) {
            f1.addChild(f2);
            return f1;
        }

        if (f2.getOperator() == FilterOperatorEnum.AND) {
            f2.addChild(f1);
            return f2;
        }

        LogicalTupleFilter and = new LogicalTupleFilter(FilterOperatorEnum.AND);
        and.addChild(f1);
        and.addChild(f2);
        return and;
    }

}
