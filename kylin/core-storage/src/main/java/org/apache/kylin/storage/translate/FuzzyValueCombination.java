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

package org.apache.kylin.storage.translate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class FuzzyValueCombination {

    private static class Dim<E> {
        TblColRef col;
        Set<E> values;
    }

    private static final Set SINGLE_NULL_SET = Sets.newHashSet();

    static {
        SINGLE_NULL_SET.add(null);
    }

    public static <E> List<Map<TblColRef, E>> calculate(Map<TblColRef, Set<E>> fuzzyValues, long cap) {
        Collections.emptyMap();
        Dim[] dims = toDims(fuzzyValues);
        // If a query has many IN clause and each IN clause has many values, then it will easily generate 
        // thousands of fuzzy keys. When there are lots of fuzzy keys, the scan performance is bottle necked 
        // on it. So simply choose to abandon all fuzzy keys in this case.
        if (exceedCap(dims, cap)) {
            return Lists.newArrayList();
        } else {
            return combination(dims);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E> List<Map<TblColRef, E>> combination(Dim[] dims) {

        List<Map<TblColRef, E>> result = Lists.newArrayList();

        int emptyDims = 0;
        for (Dim dim : dims) {
            if (dim.values.isEmpty()) {
                dim.values = SINGLE_NULL_SET;
                emptyDims++;
            }
        }
        if (emptyDims == dims.length) {
            return result;
        }

        Map<TblColRef, E> r = Maps.newHashMap();
        Iterator<E>[] iters = new Iterator[dims.length];
        int level = 0;
        while (true) {
            Dim dim = dims[level];
            if (iters[level] == null) {
                iters[level] = dim.values.iterator();
            }

            Iterator<E> it = iters[level];
            if (it.hasNext() == false) {
                if (level == 0)
                    break;
                r.remove(dim.col);
                iters[level] = null;
                level--;
                continue;
            }

            r.put(dim.col, it.next());
            if (level == dims.length - 1) {
                result.add(new HashMap<TblColRef, E>(r));
            } else {
                level++;
            }
        }
        return result;
    }

    private static <E> Dim[] toDims(Map<TblColRef, Set<E>> fuzzyValues) {
        Dim[] dims = new Dim[fuzzyValues.size()];
        int i = 0;
        for (Entry<TblColRef, Set<E>> entry : fuzzyValues.entrySet()) {
            dims[i] = new Dim();
            dims[i].col = entry.getKey();
            dims[i].values = entry.getValue();
            if (dims[i].values == null)
                dims[i].values = Collections.emptySet();
            i++;
        }
        return dims;
    }

    private static boolean exceedCap(Dim[] dims, long cap) {
        return combCount(dims) > cap;
    }

    private static long combCount(Dim[] dims) {
        long count = 1;
        for (Dim dim : dims) {
            count *= Math.max(dim.values.size(), 1);
        }
        return count;
    }

}
