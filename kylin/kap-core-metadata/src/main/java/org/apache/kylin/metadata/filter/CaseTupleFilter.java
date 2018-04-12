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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import com.google.common.collect.Lists;

/**
 * @author xjiang
 * 
 */
public class CaseTupleFilter extends TupleFilter implements IOptimizeableTupleFilter {

    private List<TupleFilter> whenFilters;
    private List<TupleFilter> thenFilters;
    private TupleFilter elseFilter;
    private Collection<?> values;
    private int filterIndex;

    public CaseTupleFilter() {
        super(new ArrayList<TupleFilter>(), FilterOperatorEnum.CASE);
        reinit();
    }

    private void reinit() {
        this.children.clear();

        this.filterIndex = 0;
        this.values = Collections.emptyList();
        this.whenFilters = new ArrayList<TupleFilter>();
        this.thenFilters = new ArrayList<TupleFilter>();
        this.elseFilter = null;
    }

    public List<TupleFilter> getWhenFilters() {
        return Collections.unmodifiableList(whenFilters);
    }

    public List<TupleFilter> getThenFilters() {
        return Collections.unmodifiableList(thenFilters);
    }

    public TupleFilter getElseFilter() {
        return elseFilter;
    }

    @Override
    public void addChild(TupleFilter child) {

        if (this.filterIndex % 2 == 0) {
            this.elseFilter = child;
        } else {
            this.whenFilters.add(this.elseFilter);
            this.thenFilters.add(child);
            this.elseFilter = null;
        }

        super.addChild(child);

        this.filterIndex++;
    }

    @Override
    public String toString() {
        return "CaseTupleFilter [when=" + whenFilters + ", then=" + thenFilters + ", else=" + elseFilter + ", children=" + children + "]";
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {

        boolean matched = false;
        for (int i = 0; i < whenFilters.size(); i++) {
            TupleFilter whenFilter = whenFilters.get(i);
            if (whenFilter.evaluate(tuple, cs)) {
                TupleFilter thenFilter = thenFilters.get(i);
                thenFilter.evaluate(tuple, cs);
                values = thenFilter.getValues();
                matched = true;
                break;
            }
        }
        if (!matched) {
            if (elseFilter != null) {
                elseFilter.evaluate(tuple, cs);
                values = elseFilter.getValues();
            } else {
                values = Collections.emptyList();
            }
        }

        return true;
    }

    @Override
    public boolean isEvaluable() {
        return false;
    }

    @Override
    public Collection<?> getValues() {
        return this.values;
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        //serialize nothing
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
    }

    @Override
    public TupleFilter acceptOptimizeTransformer(FilterOptimizeTransformer transformer) {
        List<TupleFilter> newChildren = Lists.newArrayList();
        for (TupleFilter child : this.getChildren()) {
            if (child instanceof IOptimizeableTupleFilter) {
                newChildren.add(((IOptimizeableTupleFilter) child).acceptOptimizeTransformer(transformer));
            } else {
                newChildren.add(child);
            }
        }

        this.reinit();
        this.addChildren(newChildren);

        return transformer.visit(this);
    }

}
