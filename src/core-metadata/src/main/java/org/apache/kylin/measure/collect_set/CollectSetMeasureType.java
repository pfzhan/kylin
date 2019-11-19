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

package org.apache.kylin.measure.collect_set;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class CollectSetMeasureType extends MeasureType<CollectSetCounter> {

    private final DataType dataType;
    public static final String FUNC_COLLECT_SET = "COLLECT_SET";
    public static final String DATATYPE_COLLECT_SET = "collect_set";

    public CollectSetMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public static class Factory extends MeasureTypeFactory<CollectSetCounter> {

        @Override
        public MeasureType<CollectSetCounter> createMeasureType(String funcName, DataType dataType) {
            return new CollectSetMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_COLLECT_SET;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_COLLECT_SET;
        }

        @Override
        public Class<? extends DataTypeSerializer<CollectSetCounter>> getAggrDataTypeSerializer() {
            return CollectSetSerializer.class;
        }
    }

    @Override
    public MeasureIngester<CollectSetCounter> newIngester() {
        return null;
    }

    @Override
    public MeasureAggregator<CollectSetCounter> newAggregator() {
        return null;
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.<String, Class<?>> of(
            CollectSetMeasureType.FUNC_COLLECT_SET, CollectSetAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }
}
