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
package org.apache.spark.sql.execution.datasources.sparder

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import org.apache.spark.sql.{AnalysisException, GTInfoSchema}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class SparderSchemaConverter(
    assumeBinaryIsString: Boolean =
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get,
    assumeInt96IsTimestamp: Boolean =
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get,
    writeLegacyParquetFormat: Boolean =
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get,
    primaryKey: Int,
    tableName: String) {

  def this(conf: SQLConf, primaryKey: Int, tableName: String) =
    this(
      assumeBinaryIsString = conf.isParquetBinaryAsString,
      assumeInt96IsTimestamp = conf.isParquetINT96AsTimestamp,
      writeLegacyParquetFormat = conf.writeLegacyParquetFormat,
      primaryKey = primaryKey,
      tableName = tableName
    )

  def this(conf: Configuration, primaryKey: Int, tableName: String) =
    this(
      assumeBinaryIsString =
        conf.get(SQLConf.PARQUET_BINARY_AS_STRING.key).toBoolean,
      assumeInt96IsTimestamp =
        conf.get(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key).toBoolean,
      writeLegacyParquetFormat = conf
        .get(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
             SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get.toString)
        .toBoolean,
      primaryKey = primaryKey,
      tableName = tableName
    )

  def convert(parquetSchema: MessageType): StructType =
    convert(parquetSchema.asGroupType())

  def buildFieldName(parquetSchema: GroupType, name: String): String = {
    val colId = parquetSchema.getFieldIndex(name) + primaryKey - 1
    return GTInfoSchema(tableName, colId).toString
  }

  /**
    * Converts a Parquet [[Type]] to a Spark SQL [[DataType]].
    */
  def convertField(parquetType: Type): DataType = parquetType match {
    case t: PrimitiveType => convertPrimitiveField(t)
    //scalastyle:off
    case t: GroupType     => convertGroupField(t.asGroupType())
  }

  private def convert(parquetSchema: GroupType): StructType = {
    val fields = parquetSchema.getFields.asScala
      .filter(field => !field.getName.equals("Row Key"))
      .map { field =>
        field.getRepetition match {
          case OPTIONAL =>
            StructField(buildFieldName(parquetSchema, field.getName),
                        convertField(field),
                        nullable = true)

          case REQUIRED =>
            StructField(buildFieldName(parquetSchema, field.getName),
                        convertField(field),
                        nullable = false)

          case REPEATED =>
            // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
            // annotated by `LIST` or `MAP` should be interpreted as a required list of required
            // elements where the element type is the type of the field.
            val arrayType = ArrayType(convertField(field), containsNull = false)
            StructField(buildFieldName(parquetSchema, field.getName),
                        arrayType,
                        nullable = false)
        }
      }

    StructType(fields)
  }

  private def convertPrimitiveField(field: PrimitiveType): DataType = {
    val typeName = field.getPrimitiveTypeName
    val originalType = field.getOriginalType

    def typeString =
      if (originalType == null) s"$typeName" else s"$typeName ($originalType)"

    def typeNotSupported() =
      throw new AnalysisException(s"Parquet type not supported: $typeString")

    def typeNotImplemented() =
      throw new AnalysisException(
        s"Parquet type not yet supported: $typeString")

    def illegalType() =
      throw new AnalysisException(s"Illegal Parquet type: $typeString")

    // When maxPrecision = -1, we skip precision range check, and always respect the precision
    // specified in field.getDecimalMetadata.  This is useful when interpreting decimal types stored
    // as binaries with variable lengths.
    def makeDecimalType(maxPrecision: Int = -1): DecimalType = {
      val precision = field.getDecimalMetadata.getPrecision
      val scale = field.getDecimalMetadata.getScale

      SparderSchemaConverter.checkConversionRequirement(
        maxPrecision == -1 || 1 <= precision && precision <= maxPrecision,
        s"Invalid decimal precision: $typeName cannot store $precision digits (max $maxPrecision)")

      DecimalType(precision, scale)
    }

    typeName match {
      case BOOLEAN => BooleanType

      case FLOAT => FloatType

      case DOUBLE => DoubleType
      //scalastyle:off
      case INT32 =>
        originalType match {
          case INT_8         => ByteType
          case INT_16        => ShortType
          case INT_32 | null => IntegerType
          case DATE          => DateType
          case DECIMAL       => makeDecimalType(Decimal.MAX_INT_DIGITS)
          case UINT_8        => typeNotSupported()
          case UINT_16       => typeNotSupported()
          case UINT_32       => typeNotSupported()
          case TIME_MILLIS   => typeNotImplemented()
          case _             => illegalType()
        }

      case INT64 =>
        originalType match {
          case INT_64 | null    => LongType
          case DECIMAL          => makeDecimalType(Decimal.MAX_LONG_DIGITS)
          case UINT_64          => typeNotSupported()
          case TIMESTAMP_MILLIS => typeNotImplemented()
          case _                => illegalType()
        }

      case INT96 =>
        SparderSchemaConverter.checkConversionRequirement(
          assumeInt96IsTimestamp,
          "INT96 is not supported unless it's interpreted as timestamp. " +
            s"Please try to set ${SQLConf.PARQUET_INT96_AS_TIMESTAMP.key} to true."
        )
        TimestampType

      case BINARY =>
        originalType match {
          case UTF8 | ENUM | JSON           => StringType
          case null if assumeBinaryIsString => StringType
          case null                         => BinaryType
          case BSON                         => BinaryType
          case DECIMAL                      => makeDecimalType()
          case _                            => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        originalType match {
          case DECIMAL =>
            makeDecimalType(
              SparderSchemaConverter.maxPrecisionForBytes(field.getTypeLength))
          case INTERVAL => typeNotImplemented()
          case _        => illegalType()
        }

      case _ => illegalType()
    }
  }

  private def convertGroupField(field: GroupType): DataType = {
    Option(field.getOriginalType).fold(convert(field): DataType) {
      // A Parquet list is represented as a 3-level structure:
      //
      //   <list-repetition> group <name> (LIST) {
      //     repeated group list {
      //       <element-repetition> <element-type> element;
      //     }
      //   }
      //
      // However, according to the most recent Parquet format spec (not released yet up until
      // writing), some 2-level structures are also recognized for backwards-compatibility.  Thus,
      // we need to check whether the 2nd level or the 3rd level refers to list element type.
      //
      // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
      case LIST =>
        SparderSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1,
          s"Invalid list type $field")

        val repeatedType = field.getType(0)
        SparderSchemaConverter.checkConversionRequirement(
          repeatedType.isRepetition(REPEATED),
          s"Invalid list type $field")

        if (isElementType(repeatedType, field.getName)) {
          ArrayType(convertField(repeatedType), containsNull = false)
        } else {
          val elementType = repeatedType.asGroupType().getType(0)
          val optional = elementType.isRepetition(OPTIONAL)
          ArrayType(convertField(elementType), containsNull = optional)
        }

      // scalastyle:off
      // `MAP_KEY_VALUE` is for backwards-compatibility
      // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules-1
      // scalastyle:on
      case MAP | MAP_KEY_VALUE =>
        SparderSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1 && !field.getType(0).isPrimitive,
          s"Invalid map type: $field")

        val keyValueType = field.getType(0).asGroupType()
        SparderSchemaConverter.checkConversionRequirement(
          keyValueType
            .isRepetition(REPEATED) && keyValueType.getFieldCount == 2,
          s"Invalid map type: $field")

        val keyType = keyValueType.getType(0)
        SparderSchemaConverter.checkConversionRequirement(
          keyType.isPrimitive,
          s"Map key type is expected to be a primitive type, but found: $keyType")

        val valueType = keyValueType.getType(1)
        val valueOptional = valueType.isRepetition(OPTIONAL)
        MapType(convertField(keyType),
                convertField(valueType),
                valueContainsNull = valueOptional)

      case _ =>
        throw new AnalysisException(s"Unrecognized Parquet type: $field")
    }
  }

  // scalastyle:off
  // Here we implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // scalastyle:on
  private def isElementType(repeatedType: Type, parentName: String): Boolean = {
    {
      // For legacy 2-level list types with primitive element type, e.g.:
      //
      //    // ARRAY<INT> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated int32 element;
      //    }
      //
      repeatedType.isPrimitive
    } || {
      // For legacy 2-level list types whose element type is a group type with 2 or more fields,
      // e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group element {
      //        required binary str (UTF8);
      //        required int32 num;
      //      };
      //    }
      //
      repeatedType.asGroupType().getFieldCount > 1
    } || {
      // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group array {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == "array"
    } || {
      // For Parquet data generated by parquet-thrift, e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group my_list_tuple {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == s"${parentName}_tuple"
    }
  }

}

object SparderSchemaConverter {
  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new AnalysisException(message)
    }
  }

  // Max precision of a decimal value stored in `numBytes` bytes
  def maxPrecisionForBytes(numBytes: Int): Int = {
    Math
      .round( // convert double to long
        Math.floor(Math.log10( // number of base-10 digits
          Math.pow(2, 8 * numBytes - 1) - 1))) // max value stored in numBytes
      .asInstanceOf[Int]
  }
}
