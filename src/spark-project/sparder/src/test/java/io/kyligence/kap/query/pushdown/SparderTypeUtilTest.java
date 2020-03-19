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

package io.kyligence.kap.query.pushdown;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.junit.Assert;
import org.junit.Test;

public class SparderTypeUtilTest {

    private RelDataType baiscSqlType(SqlTypeName typeName) {
        return new BasicSqlType(RelDataTypeSystem.DEFAULT, typeName);
    }

    private String convertToStringWithCalciteType(Object value, SqlTypeName typeName) {
        return SparderTypeUtil.convertToStringWithCalciteType(value, baiscSqlType(typeName));
    }

    private String convertToStringWithDecimalType(Object value, int precision, int scale) {
        return SparderTypeUtil.convertToStringWithCalciteType(value, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, precision, scale));
    }

    @Test
    public void testConvertToStringWithCalciteType() {
        // matched types
        Assert.assertEquals("9", convertToStringWithCalciteType(Integer.valueOf("9"), SqlTypeName.INTEGER));
        Assert.assertEquals("9", convertToStringWithCalciteType(Short.valueOf("9"), SqlTypeName.SMALLINT));
        Assert.assertEquals("9", convertToStringWithCalciteType(Byte.valueOf("9"), SqlTypeName.TINYINT));
        Assert.assertEquals("9", convertToStringWithCalciteType(Long.valueOf("9"), SqlTypeName.BIGINT));
        Assert.assertEquals("123.345", convertToStringWithDecimalType(new BigDecimal("123.345"), 29, 3));
        Assert.assertEquals("2012-01-01", convertToStringWithCalciteType(java.sql.Date.valueOf("2012-01-01"), SqlTypeName.DATE));
        Assert.assertEquals("2012-01-01 12:34:56", convertToStringWithCalciteType(java.sql.Timestamp.valueOf("2012-01-01 12:34:56"), SqlTypeName.TIMESTAMP));
        // cast to char/varchar
        Assert.assertEquals("foo", convertToStringWithCalciteType("foo", SqlTypeName.VARCHAR));
        Assert.assertEquals("foo", convertToStringWithCalciteType("foo", SqlTypeName.CHAR));
        Assert.assertEquals("123.345", convertToStringWithCalciteType(new BigDecimal("123.345"), SqlTypeName.VARCHAR));
        Assert.assertEquals("2012-01-01", convertToStringWithCalciteType(java.sql.Date.valueOf("2012-01-01"), SqlTypeName.VARCHAR));
        Assert.assertEquals("2012-01-01 12:34:56", convertToStringWithCalciteType(java.sql.Timestamp.valueOf("2012-01-01 12:34:56"), SqlTypeName.VARCHAR));
        // type conversion
        Assert.assertEquals("9", convertToStringWithCalciteType(Float.valueOf("9.1"), SqlTypeName.INTEGER));
        Assert.assertEquals("9", convertToStringWithCalciteType(Double.valueOf("9.1"), SqlTypeName.TINYINT));
        Assert.assertEquals("9", convertToStringWithCalciteType("9.1", SqlTypeName.SMALLINT));
        Assert.assertEquals("9", convertToStringWithCalciteType(new BigDecimal("9.1"), SqlTypeName.BIGINT));
        Assert.assertEquals("9", convertToStringWithDecimalType(Long.valueOf("9"), 18, 0));
        Assert.assertEquals("9.123", convertToStringWithDecimalType(Double.valueOf("9.123"), 18, 3));
        Assert.assertEquals("9.1", convertToStringWithDecimalType(Double.valueOf("9.123"), 18, 1));
        Assert.assertEquals("2012-01-01 00:00:00", convertToStringWithCalciteType(java.sql.Date.valueOf("2012-01-01"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01", convertToStringWithCalciteType(java.sql.Timestamp.valueOf("2012-01-01 12:34:56"), SqlTypeName.DATE));
    }

    @Test
    public void testEmpty() {
        JavaTypeFactoryImpl tp = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(BigDecimal.class), false)
                .equals(new BigDecimal(0)));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(String.class), false).equals(""));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(char.class), false).equals(""));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(short.class), false).equals((short)0));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(int.class), false).equals(0));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(long.class), false).equals(0L));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(double.class), false).equals(0D));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(float.class), false).equals(0F));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(boolean.class), false) == null);
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(Date.class), false).equals(0));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(Time.class), false).equals(0L));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(Timestamp.class), false).equals(0L));
    }

    @Test
    public void testUnMatchType() {
        JavaTypeFactoryImpl tp = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(BigDecimal.class), false)
                .equals(new BigDecimal(0)));
        Double value = 0.604951272091475354;
        Object convert_value =
                SparderTypeUtil.convertStringToValue(value, tp.createType(long.class), false);
        Assert.assertEquals(Long.class, convert_value.getClass());
        Assert.assertEquals(0L, convert_value);
    }
}