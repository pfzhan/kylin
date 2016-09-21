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

package io.kyligence.kap.metadata.filter;

import static org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum.EVAL_FUNC;

import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;

public class TupleFilterSerializerExt extends TupleFilterSerializer {

    static {
        extendedTupleFilters.put(EVAL_FUNC, EvaluatableFunctionTupleFilter.class);
    }

    public static TupleFilter deserialize(byte[] bytes, IFilterCodeSystem<?> cs) {
        return TupleFilterSerializer.deserialize(bytes, cs);
    }
}
