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
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

import io.kyligence.kap.storage.parquet.format.raw.RawTableUtils;

public class TupleFilterLiteralHasher implements TupleFilterSerializer.Decorator {
    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (!(filter instanceof CompareTupleFilter)) {
            return filter;
        }

        CompareTupleFilter oldCompareFilter = (CompareTupleFilter) filter;

        // extract ColumnFilter & ConstantFilter
        TblColRef externalCol = oldCompareFilter.getColumn();

        if (externalCol == null) {
            return oldCompareFilter;
        }

        Collection constValues = oldCompareFilter.getValues();
        if (constValues == null || constValues.isEmpty()) {
            return oldCompareFilter;
        }

        //CompareTupleFilter containing BuiltInFunctionTupleFilter will not reach here caz it will be transformed by BuiltInFunctionTransformer
        CompareTupleFilter newCompareFilter = new CompareTupleFilter(oldCompareFilter.getOperator());
        newCompareFilter.addChild(new ColumnTupleFilter(externalCol));

        //for CompareTupleFilter containing dynamicVariables, the below codes will actually replace dynamicVariables
        //with normal ConstantTupleFilter

        Set<ByteArray> newValues = Sets.newHashSet();
        for (ByteArray value : (Collection<ByteArray>) constValues) {
            newValues.add(RawTableUtils.hash(value));
        }
        newCompareFilter.addChild(new ConstantTupleFilter(newValues));
        return newCompareFilter;
    }
}
