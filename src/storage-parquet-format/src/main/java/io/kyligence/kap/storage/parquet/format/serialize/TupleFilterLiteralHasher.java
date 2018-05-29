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
