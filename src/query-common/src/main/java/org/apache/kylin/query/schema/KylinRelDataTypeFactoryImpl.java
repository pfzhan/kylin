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
package org.apache.kylin.query.schema;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import lombok.EqualsAndHashCode;

public class KylinRelDataTypeFactoryImpl extends JavaTypeFactoryImpl {

    private static final LoadingCache<Object, RelDataType> CACHE = CacheBuilder.newBuilder().softValues()
            .build(new CacheLoader<Object, RelDataType>() {
                @Override
                public RelDataType load(@Nonnull Object k) {
                    if (k instanceof RelDataType) {
                        return (RelDataType) k;
                    }
                    @SuppressWarnings("unchecked")
                    final Key key = (Key) k;
                    final ImmutableList.Builder<RelDataTypeField> list = ImmutableList.builder();
                    for (int i = 0; i < key.names.size(); i++) {
                        list.add(new KylinRelDataTypeFieldImpl(key.names.get(i), i, key.types.get(i),
                                key.columnTypes.get(i)));
                    }
                    return new RelRecordType(key.kind, list.build());
                }
            });

    protected KylinRelDataTypeFactoryImpl(RelDataTypeFactory typeFactory) {
        super(typeFactory.getTypeSystem());
    }

    public RelDataType createStructType(StructKind kind, List<RelDataType> typeList, List<String> fieldNameList,
            List<KylinRelDataTypeFieldImpl.ColumnType> columnTypes) {
        final RelDataType type = CACHE.getIfPresent(new Key(kind, fieldNameList, typeList, columnTypes));
        if (type != null) {
            return type;
        }
        final ImmutableList<String> names = ImmutableList.copyOf(fieldNameList);
        final ImmutableList<RelDataType> types = ImmutableList.copyOf(typeList);
        final ImmutableList<KylinRelDataTypeFieldImpl.ColumnType> colTypes = ImmutableList.copyOf(columnTypes);
        return CACHE.getUnchecked(new Key(kind, names, types, colTypes));
    }

    /** Key to the data type cache. */
    @EqualsAndHashCode
    private static class Key {
        private final StructKind kind;
        private final List<String> names;
        private final List<RelDataType> types;
        private final List<KylinRelDataTypeFieldImpl.ColumnType> columnTypes;

        Key(StructKind kind, List<String> names, List<RelDataType> types,
                List<KylinRelDataTypeFieldImpl.ColumnType> columnTypes) {
            this.kind = kind;
            this.names = names;
            this.types = types;
            this.columnTypes = columnTypes;
        }
    }
}
