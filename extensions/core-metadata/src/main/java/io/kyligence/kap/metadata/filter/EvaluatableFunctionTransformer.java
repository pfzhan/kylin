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

import java.util.ListIterator;

import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.EvaluatableFunctionTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
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
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) tupleFilter.getChildren().listIterator();
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
        return translated;
    }

    private static TupleFilter translateCompareTupleFilter(CompareTupleFilter compTupleFilter) {

        if (compTupleFilter.getFunction() == null || (!(compTupleFilter.getFunction() instanceof BuiltInFunctionTupleFilter))) {
            return null;
        }

        //TODO: currently not handling BuiltInFunctionTupleFilter in compare filter, like lower(s) > 'a'
        if (!"like".equals(((BuiltInFunctionTupleFilter) compTupleFilter.getFunction()).getName())) {
            return null;
        }

        BuiltInFunctionTupleFilter builtInFunctionTupleFilter = (BuiltInFunctionTupleFilter) compTupleFilter.getFunction();

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