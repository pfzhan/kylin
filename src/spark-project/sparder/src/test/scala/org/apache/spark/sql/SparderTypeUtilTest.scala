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
package org.apache.spark.sql

import java.sql.Types

import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.sql.`type`.SqlTypeFactoryImpl
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.query.schema.OLAPTable
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.util.SparderTypeUtil

class SparderTypeUtilTest extends SparderBaseFunSuite {
  val dataTypes = List(DataType.getType("decimal(19,4)"),
    DataType.getType("char(50)"),
    DataType.getType("varchar(1000)"),
    DataType.getType("date"),
    DataType.getType("timestamp"),
    DataType.getType("tinyint"),
    DataType.getType("smallint"),
    DataType.getType("integer"),
    DataType.getType("bigint"),
    DataType.getType("float"),
    DataType.getType("double"),
    DataType.getType("decimal(38,19)")
  )


  test("Test decimal") {
    val dt = DataType.getType("decimal(19,4)")
    val dataTp = DataTypes.createDecimalType(19, 4)
    val dataType = SparderTypeUtil.kylinTypeToSparkResultType(dt)
    assert(dataTp.sameType(dataType))
    val sparkTp = SparderTypeUtil.toSparkType(dt)
    assert(dataTp.sameType(sparkTp))
    val sparkTpSum = SparderTypeUtil.toSparkType(dt, true)
    assert(DataTypes.createDecimalType(29, 4).sameType(sparkTpSum))
  }

  test("Test kylinRawTableSQLTypeToSparkType") {
    dataTypes.map(SparderTypeUtil.kylinRawTableSQLTypeToSparkType)

  }

  test("Test kylinTypeToSparkResultType") {
    dataTypes.map(SparderTypeUtil.kylinTypeToSparkResultType)
  }

  test("Test toSparkType") {
    dataTypes.map(dt => {
      val sparkType = SparderTypeUtil.toSparkType(dt)
      SparderTypeUtil.convertSparkTypeToSqlType(sparkType)
    })
  }

  test("Test convertSqlTypeToSparkType") {
    val typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
    dataTypes.map(dt => {
      val relDataType = OLAPTable.createSqlType(typeFactory, dt, true)
      SparderTypeUtil.convertSqlTypeToSparkType(relDataType)
    })
  }

  test("test convertSparkFieldToJavaField") {
    val typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
    dataTypes.map(dt => {
      val relDataType = OLAPTable.createSqlType(typeFactory, dt, true)
      val structField = SparderTypeUtil.convertSparkFieldToJavaField(
        StructField("foo", SparderTypeUtil.convertSqlTypeToSparkType(relDataType))
      )

      if (relDataType.getSqlTypeName.getJdbcOrdinal == Types.CHAR) {
        assert(Types.VARCHAR == structField.getDataType)
      } else if (relDataType.getSqlTypeName.getJdbcOrdinal == Types.DECIMAL) {
        assert(Types.DECIMAL == structField.getDataType)
        assert(relDataType.getPrecision == structField.getPrecision)
        assert(relDataType.getScale == structField.getScale)
      } else {
        assert(relDataType.getSqlTypeName.getJdbcOrdinal == structField.getDataType)
      }
    })
  }

}
