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

package io.kyligence.kap.storage.parquet.format.serialize;

import java.util.Collection;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;

import com.google.common.collect.Sets;

import io.kyligence.kap.storage.parquet.format.raw.RawTableUtils;

public class TupleFilterLiteralHasher implements TupleFilterSerializer.Decorator {
    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (filter instanceof ConstantTupleFilter) {
            ConstantTupleFilter constantTupleFilter = (ConstantTupleFilter) filter;
            Set<ByteArray> newValues = Sets.newHashSet();

            for (ByteArray value : (Collection<ByteArray>) constantTupleFilter.getValues()) {
                newValues.add(RawTableUtils.hash(value));
            }
            return new ConstantTupleFilter(newValues);
        }
        return filter;
    }
}
