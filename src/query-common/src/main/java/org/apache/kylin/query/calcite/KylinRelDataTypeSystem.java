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

package org.apache.kylin.query.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.common.KapConfig;

@SuppressWarnings("unused") //used by reflection
public class KylinRelDataTypeSystem extends RelDataTypeSystemImpl {
    @Override
    public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        if (argumentType instanceof BasicSqlType) {
            switch (argumentType.getSqlTypeName()) {
                case DECIMAL:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.DECIMAL,
                                    argumentType.getPrecision() + 4,
                                    argumentType.getScale() + 4),
                            argumentType.isNullable()
                    );
                default:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.DOUBLE),
                            argumentType.isNullable()
                    );
            }
        }
        return super.deriveAvgAggType(typeFactory, argumentType);
    }

    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        if (argumentType instanceof BasicSqlType) {
            switch (argumentType.getSqlTypeName()) {
                case INTEGER:
                case SMALLINT:
                case TINYINT:
                    return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
                            argumentType.isNullable());
                case DECIMAL:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.DECIMAL,
                                    Math.max(19, argumentType.getPrecision()),
                                    argumentType.getScale()),
                            argumentType.isNullable());
                default:
                    break;
            }
        }
        return argumentType;
    }

    /**
     * Hive support decimal with 38 digits, kylin should align
     *
     * @see org.apache.calcite.rel.type.RelDataTypeSystem
     * @see <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-DecimalsdecimalDecimals">Hive/LanguageManualTypes-Decimals</a>
     */
    @Override
    public int getMaxNumericPrecision() {
        return 38;
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return true;
    }

    @Override
    public int getDefaultScale(SqlTypeName typeName) {
        switch (typeName) {
            case DECIMAL:
                return KapConfig.getInstanceFromEnv().defaultDecimalScale();
            default:
                return super.getDefaultScale(typeName);
        }
    }
}
