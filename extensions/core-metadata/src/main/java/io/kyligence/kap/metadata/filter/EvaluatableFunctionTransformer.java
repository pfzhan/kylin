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

package io.kyligence.kap.metadata.filter;

import java.util.ListIterator;

import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvaluatableFunctionTransformer {
    public static final Logger logger = LoggerFactory.getLogger(EvaluatableFunctionTransformer.class);

    public static TupleFilter transform(TupleFilter tupleFilter) {
        TupleFilter translated = null;

        if (tupleFilter instanceof CompareTupleFilter) {
            translated = translateCompareTupleFilter((CompareTupleFilter) tupleFilter);
        } else if (tupleFilter instanceof BuiltInFunctionTupleFilter) {
            translated = translateFunctionTupleFilter((BuiltInFunctionTupleFilter) tupleFilter);
        } else if (tupleFilter instanceof LogicalTupleFilter) {
            @SuppressWarnings("unchecked")
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) tupleFilter.getChildren()
                    .listIterator();
            while (childIterator.hasNext()) {
                TupleFilter transformed = transform(childIterator.next());
                if (transformed != null) {
                    childIterator.set(transformed);
                } else {
                    throw new IllegalStateException("Should not be null");
                }
            }
        }
        return translated == null ? tupleFilter : translated;
    }

    private static TupleFilter translateFunctionTupleFilter(BuiltInFunctionTupleFilter functionTupleFilter) {
        if (!functionTupleFilter.isValid())
            return null;

        EvaluatableFunctionTupleFilter translated = new EvaluatableFunctionTupleFilter(functionTupleFilter.getName());
        for (TupleFilter child : functionTupleFilter.getChildren()) {
            TupleFilter transformed = transform(child);
            translated.addChild(transformed);
        }
        translated.setReversed(functionTupleFilter.isReversed());

        return translated;
    }

    private static TupleFilter translateCompareTupleFilter(CompareTupleFilter compTupleFilter) {

        if (compTupleFilter.getFunction() == null
                || (!(compTupleFilter.getFunction() instanceof BuiltInFunctionTupleFilter))) {
            return null;
        }

        //TODO: currently not handling BuiltInFunctionTupleFilter in compare filter, like lower(s) > 'a'
        if (!"like".equals(((BuiltInFunctionTupleFilter) compTupleFilter.getFunction()).getName())) {
            return null;
        }

        BuiltInFunctionTupleFilter builtInFunctionTupleFilter = (BuiltInFunctionTupleFilter) compTupleFilter
                .getFunction();

        if (!builtInFunctionTupleFilter.isValid())
            return null;

        CompareTupleFilter translated = new CompareTupleFilter(compTupleFilter.getOperator());
        for (TupleFilter child : compTupleFilter.getChildren()) {
            TupleFilter transformedChild = transform(child);
            translated.addChild(transformedChild);
        }

        return translated;
    }

}
