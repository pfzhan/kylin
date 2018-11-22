/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.sql.types._

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class ParquetSchemaUtilsSuite extends UnitTestSuite {
  test("validateStructType - empty schema") {
    val err = intercept[UnsupportedOperationException] {
      ParquetSchemaUtils.validateStructType(StructType(Nil))
    }
    assert(err.getMessage.contains("Empty schema StructType() is not supported"))
  }

  test("validateStructType - contains unsupported field") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", StringType) ::
      StructField("c", StructType(
        StructField("c1", IntegerType) :: Nil)
      ) :: Nil)
    val err = intercept[UnsupportedOperationException] {
      ParquetSchemaUtils.validateStructType(schema)
    }
    assert(err.getMessage.contains("Schema contains unsupported type, " +
      "field=StructField(c,StructType(StructField(c1,IntegerType,true)),true)"))
  }

  test("validateStructType - all fields are ok") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", StringType) ::
      StructField("c", LongType) :: Nil)
    ParquetSchemaUtils.validateStructType(schema)
  }

  test("pruneStructType - empty schema") {
    val schema = StructType(Nil)
    ParquetSchemaUtils.pruneStructType(schema) should be (schema)
  }

  test("pruneStructType - all supported types") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) ::
      StructField("c", StringType) :: Nil)
    ParquetSchemaUtils.pruneStructType(schema) should be (schema)
  }

  test("pruneStructType - mix of supported and not supported types") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) ::
      StructField("c", StringType) ::
      StructField("d", BooleanType) ::
      StructField("e", ArrayType(LongType)) :: Nil)

    val expected = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) ::
      StructField("c", StringType) :: Nil)

    ParquetSchemaUtils.pruneStructType(schema) should be (expected)
  }

  test("pruneStructType - all not supported types") {
    val schema = StructType(
      StructField("a", ArrayType(IntegerType)) ::
      StructField("b", StructType(
        StructField("c", LongType) ::
        StructField("d", StringType) :: Nil)
      ) :: Nil)

    ParquetSchemaUtils.pruneStructType(schema) should be (StructType(Nil))
  }

  test("topLevelUniqueColumns - extract primitive columns for unique fields") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required boolean flag;
      |   required int64 id;
      |   required int32 index;
      |   required binary str (UTF8);
      | }
      """.stripMargin)
    ParquetSchemaUtils.topLevelUniqueColumns(schema) should be (
      ("flag", 0) :: ("id", 1) :: ("index", 2) :: ("str", 3) :: Nil)
  }

  test("topLevelUniqueColumns - extract mixed columns for unique fields") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required int64 id;
      |   required binary str (UTF8);
      |   optional group b {
      |     optional binary _1 (UTF8);
      |     required boolean _2;
      |   }
      |   optional group c (LIST) {
      |     repeated group list {
      |       required int32 element;
      |     }
      |   }
      | }
      """.stripMargin)
    ParquetSchemaUtils.topLevelUniqueColumns(schema) should be (
      ("id", 0) :: ("str", 1) :: ("b", 2) :: ("c", 3) :: Nil)
  }

  test("topLevelUniqueColumns - empty schema") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema { }
      """.stripMargin)
    ParquetSchemaUtils.topLevelUniqueColumns(schema) should be (Nil)
  }

  test("topLevelUniqueColumns - duplicate column names") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required int64 id;
      |   required int32 id;
      |   required binary str (UTF8);
      | }
      """.stripMargin)
    val err = intercept[IllegalArgumentException] {
      ParquetSchemaUtils.topLevelUniqueColumns(schema)
    }
    assert(err.getMessage.contains("[required int32 id] with duplicate column name 'id'"))
  }

  test("merge - two identical schemas") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "b").build)
      .add("col2", StringType, true, new MetadataBuilder().putString("c", "d").build)
    val schema2 = schema1

    ParquetSchemaUtils.merge(schema1, schema2) should be (schema1)
  }

  test("merge - two different schemas") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
    val schema2 = StructType(Nil)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "1").build)

    val result = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "1").build)

    ParquetSchemaUtils.merge(schema1, schema2) should be (result)
  }

  test("merge - two interleaved schemas") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, false, new MetadataBuilder().putString("b", "1").build)
    val schema2 = StructType(Nil)
      .add("col2", StringType, true, new MetadataBuilder().putString("c", "1").build)

    val result = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, true,
        new MetadataBuilder().putString("b", "1").putString("c", "1").build)

    ParquetSchemaUtils.merge(schema1, schema2) should be (result)
  }

  test("merge - two interleaved schemas with merged metadata for the same key") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, false, new MetadataBuilder().putString("b", "1").build)
    val schema2 = StructType(Nil)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "2").build)

    val result = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "2").build)

    ParquetSchemaUtils.merge(schema1, schema2) should be (result)
  }
}
