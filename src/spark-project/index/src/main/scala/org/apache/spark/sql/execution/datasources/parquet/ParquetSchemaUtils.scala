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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.types._

/**
 * Utility functions to provide list of supported fields, validate and prune columns.
 */
object ParquetSchemaUtils {
  // Supported top-level Spark SQL data types
  val SUPPORTED_TYPES: Set[DataType] =
    Set(IntegerType, LongType, StringType, DateType, TimestampType)

  /**
   * Validate input schema as `StructType` and throw exception if schema does not match expected
   * column types or is empty (see list of supported fields above). Note that this is used in
   * statistics conversion, so when adding new type, one should update statistics.
   */
  def validateStructType(schema: StructType): Unit = {
    if (schema.isEmpty) {
      throw new UnsupportedOperationException(s"Empty schema $schema is not supported, please " +
        s"provide at least one column of a type ${SUPPORTED_TYPES.mkString("[", ", ", "]")}")
    }
    schema.fields.foreach { field =>
      if (!SUPPORTED_TYPES.contains(field.dataType)) {
        throw new UnsupportedOperationException(
          "Schema contains unsupported type, " +
          s"field=$field, " +
          s"schema=${schema.simpleString}, " +
          s"supported types=${SUPPORTED_TYPES.mkString("[", ", ", "]")}")
      }
    }
  }

  /**
   * Prune invalid columns from StructType leaving only supported data types. Only works for
   * top-level columns at this point.
   */
  def pruneStructType(schema: StructType): StructType = {
    val updatedFields = schema.fields.filter { field =>
      SUPPORTED_TYPES.contains(field.dataType)
    }
    StructType(updatedFields)
  }

  /**
   * Extract top level columns from schema and return them as (field name - field index) pairs.
   * This does not contain duplicate pairs, neither pairs with different index but the same column
   * name.
   */
  def topLevelUniqueColumns(schema: MessageType): Seq[(String, Int)] = {
    // make sure that names are unique for top level columns
    val uniqueColumns = mutable.HashSet[String]()
    schema.getFields.asScala.map { field =>
      if (uniqueColumns.contains(field.getName)) {
        throw new IllegalArgumentException(s"""
          | Found field [$field] with duplicate column name '${field.getName}'.
          | Schema $schema
          | This situation is currently not supported, ensure that names of all top level columns
          | in schema are unique""".stripMargin)
      }
      uniqueColumns.add(field.getName)
      val index = schema.getFieldIndex(field.getName)
      (field.getName, index)
    }
  }

  /** Update field with provided metadata, performs replacement, not merge */
  private def withMetadata(field: StructField, metadata: Metadata): StructField = {
    StructField(field.name, field.dataType, field.nullable, metadata)
  }

  /**
   * Merge schemas with preserved metadata for top-level fields.
   * TODO: implement merge for nested types.
   */
  def merge(schema1: StructType, schema2: StructType): StructType = {
    // perform field merge, this does not merge metadata
    val mergedSchema = schema1.merge(schema2)

    // update field with extracted and merged metadata
    val updatedFields = mergedSchema.map { field =>
      val field1 = Try(schema1(field.name)).toOption
      val field2 = Try(schema2(field.name)).toOption

      (field1, field2) match {
        case (Some(value1), Some(value2)) =>
          val metadata = new MetadataBuilder().
            withMetadata(value1.metadata).
            withMetadata(value2.metadata).build
          if (metadata == field.metadata) field else withMetadata(field, metadata)
        case (Some(value1), None) =>
          if (value1 == field) field else withMetadata(field, value1.metadata)
        case (None, Some(value2)) =>
          if (value2 == field) field else withMetadata(field, value2.metadata)
        case other =>
          field
      }
    }

    // return final merged schema
    StructType(updatedFields)
  }
}
