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

package org.apache.kylin.dict;

import java.util.Collection;
import java.util.ListIterator;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.ITupleFilterTransformer;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;

/**
 * only take effect when the compare filter has function
 * 
 * is a first type transformer defined in ITupleFilterTransformer
 */
public class BuiltInFunctionTransformer implements ITupleFilterTransformer {
    public static final Logger logger = LoggerFactory.getLogger(BuiltInFunctionTransformer.class);

    private IDimensionEncodingMap dimEncMap;

    public BuiltInFunctionTransformer(IDimensionEncodingMap dimEncMap) {
        this.dimEncMap = dimEncMap;
    }

    @Override
    public TupleFilter transform(TupleFilter tupleFilter) {
        TupleFilter translated = null;
        if (tupleFilter instanceof CompareTupleFilter) {
            //normal case
            translated = translateCompareTupleFilter((CompareTupleFilter) tupleFilter);
            if (translated != null) {
                logger.debug("Translated {{}} to IN clause. ", tupleFilter);
            }
        } else if (tupleFilter instanceof BuiltInFunctionTupleFilter) {
            //like,tolower case
            translated = translateFunctionTupleFilter((BuiltInFunctionTupleFilter) tupleFilter);
            if (translated != null) {
                logger.debug("Translated {{}} to IN clause. ", tupleFilter);
            }
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

    private TupleFilter translateFunctionTupleFilter(BuiltInFunctionTupleFilter builtInFunctionTupleFilter) {
        if (!builtInFunctionTupleFilter.isValid())
            return null;

        TblColRef columnRef = builtInFunctionTupleFilter.getColumn();
        Dictionary<?> dict = dimEncMap.getDictionary(columnRef);
        if (dict == null)
            return null;

        CompareTupleFilter translated = new CompareTupleFilter(builtInFunctionTupleFilter.isReversed() ? FilterOperatorEnum.NOTIN : FilterOperatorEnum.IN);
        translated.addChild(new ColumnTupleFilter(columnRef));

        try {
            int translatedInClauseMaxSize = KylinConfig.getInstanceFromEnv().getTranslatedInClauseMaxSize();

            for (int i = dict.getMinId(); i <= dict.getMaxId(); i++) {
                Object dictVal = dict.getValueFromId(i);
                if ((Boolean) builtInFunctionTupleFilter.invokeFunction(dictVal)) {
                    translated.addChild(new ConstantTupleFilter(dictVal));

                    if (translated.getChildren().size() > translatedInClauseMaxSize) {
                        return null;
                    }
                }
            }
            logger.debug("getting a in clause with {} children", translated.getChildren().size());
        } catch (Exception e) {
            logger.debug(e.getMessage());
            return null;
        }
        return translated;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private TupleFilter translateCompareTupleFilter(CompareTupleFilter compTupleFilter) {
        if (compTupleFilter.getFunction() == null || (!(compTupleFilter.getFunction() instanceof BuiltInFunctionTupleFilter)))
            return null;

        BuiltInFunctionTupleFilter builtInFunctionTupleFilter = (BuiltInFunctionTupleFilter) compTupleFilter.getFunction();

        if (!builtInFunctionTupleFilter.isValid())
            return null;

        TblColRef columnRef = builtInFunctionTupleFilter.getColumn();
        Dictionary<?> dict = dimEncMap.getDictionary(columnRef);
        if (dict == null)
            return null;

        CompareTupleFilter translated = new CompareTupleFilter(builtInFunctionTupleFilter.isReversed() ? FilterOperatorEnum.NOTIN : FilterOperatorEnum.IN);
        translated.addChild(new ColumnTupleFilter(columnRef));

        try {
            Collection<Object> inValues = Lists.newArrayList();
            for (int i = dict.getMinId(); i <= dict.getMaxId(); i++) {
                Object dictVal = dict.getValueFromId(i);
                Object computedVal = builtInFunctionTupleFilter.invokeFunction(dictVal);
                Class clazz = Primitives.wrap(computedVal.getClass());
                Object targetVal = compTupleFilter.getFirstValue();
                if (Primitives.isWrapperType(clazz))
                    targetVal = clazz.cast(clazz.getDeclaredMethod("valueOf", String.class).invoke(null, compTupleFilter.getFirstValue()));

                int comp = ((Comparable) computedVal).compareTo(targetVal);
                boolean compResult = false;
                switch (compTupleFilter.getOperator()) {
                case EQ:
                    compResult = comp == 0;
                    break;
                case NEQ:
                    compResult = comp != 0;
                    break;
                case LT:
                    compResult = comp < 0;
                    break;
                case LTE:
                    compResult = comp <= 0;
                    break;
                case GT:
                    compResult = comp > 0;
                    break;
                case GTE:
                    compResult = comp >= 0;
                    break;
                case IN:
                    compResult = compTupleFilter.getValues().contains(computedVal.toString());
                    break;
                case NOTIN:
                    compResult = !compTupleFilter.getValues().contains(computedVal.toString());
                    break;
                default:
                    break;
                }
                if (compResult) {
                    inValues.add(dictVal);
                }
            }
            translated.addChild(new ConstantTupleFilter(inValues));
        } catch (Exception e) {
            logger.debug(e.getMessage());
            return null;
        }
        return translated;
    }
}
