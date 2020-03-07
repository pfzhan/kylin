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
package org.apache.spark.sql

import io.kyligence.kap.metadata.cube.model.LayoutEntity
import org.apache.kylin.metadata.model.FunctionDesc
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.JavaConverters._


object LayoutEntityConverter {
  implicit class LayoutEntityConverter(layoutEntity: LayoutEntity) {
    def toCatalogTable(): CatalogTable = {
      val partitionColumn = layoutEntity.getPartitionByColumns.asScala.map(_.toString)
      val bucketSp = {
        if (!layoutEntity.getShardByColumns.isEmpty) {
          BucketSpec(layoutEntity.getBucketNum,
            layoutEntity.getShardByColumns.asScala.map(_.toString),
            layoutEntity.getColOrder.asScala.map(_.toString).filter(!partitionColumn.contains(_)))
        } else {
          null
        }
      }
      CatalogTable(identifier = TableIdentifier(s"${layoutEntity.getId}", Option(layoutEntity.getModel.getId)),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty,
        schema = genCuboidSchemaFromNCuboidLayoutWithPartitionColumn(layoutEntity, partitionColumn),
        partitionColumnNames = partitionColumn,
        bucketSpec = Option(bucketSp))
    }

    def toSchema() : StructType = {
      genCuboidSchemaFromNCuboidLayout(layoutEntity)
    }

    def toExactlySchema() : StructType = {
      genCuboidSchemaFromNCuboidLayout(layoutEntity, true)
    }
  }

  def genCuboidSchemaFromNCuboidLayoutWithPartitionColumn(cuboid: LayoutEntity, partitionColumn: Seq[String]): StructType = {
    val dimensions = cuboid.getOrderedDimensions
    StructType(dimensions.asScala.filter(tp => !partitionColumn.contains(tp._1.toString)).map { i =>
      StructField(
        i._1.toString,
        SparderTypeUtil.toSparkType(i._2.getType),
        nullable = true
      )
    }.toSeq ++
      cuboid.getOrderedMeasures.asScala.map {
        i =>
          StructField(
            i._1.toString,
            generateFunctionReturnDataType(i._2.getFunction),
            nullable = true)
      }.toSeq ++
      partitionColumn.map(pt => (pt, dimensions.get(pt.toInt))).map { i =>
        StructField(
          i._1.toString,
          SparderTypeUtil.toSparkType(i._2.getType),
          nullable = true
        )
      })
  }

  def genCuboidSchemaFromNCuboidLayout(cuboid: LayoutEntity, isFastBitmapEnabled: Boolean = false): StructType = {
    val measures = if (isFastBitmapEnabled) {
      val countDistinctColumns = cuboid.listBitmapMeasure().asScala
      cuboid.getOrderedMeasures.asScala.map {
        i =>
          if (countDistinctColumns.contains(i._1.toString)) {
            StructField(i._1.toString, LongType, nullable = true)
          } else {
            StructField(
              i._1.toString,
              generateFunctionReturnDataType(i._2.getFunction),
              nullable = true)
          }
      }.toSeq
    } else {
      cuboid.getOrderedMeasures.asScala.map {
        i =>
          StructField(
            i._1.toString,
            generateFunctionReturnDataType(i._2.getFunction),
            nullable = true)
      }.toSeq
    }

      StructType(cuboid.getOrderedDimensions.asScala.map { i =>
      StructField(
        i._1.toString,
        SparderTypeUtil.toSparkType(i._2.getType),
        nullable = true
      )
    }.toSeq ++ measures)
  }
  def genBucketSpec(layoutEntity: LayoutEntity, partitionColumn: Set[String]): Option[BucketSpec] = {
    if (layoutEntity.getShardByColumns.isEmpty) {
      Option(BucketSpec(layoutEntity.getBucketNum,
        layoutEntity.getShardByColumns.asScala.map(_.toString),
        layoutEntity.getColOrder.asScala.map(_.toString).filter(!partitionColumn.contains(_))))
    } else {
      Option(null)
    }
  }


  def generateFunctionReturnDataType(function: FunctionDesc): DataType = {
    function.getExpression.toUpperCase match {
      case "SUM" =>
        val parameter = function.getParameters.get(0)
        if (parameter.isColumnType) {
          SparderTypeUtil.toSparkType(parameter.getColRef.getType, true)
        } else {
          SparderTypeUtil.toSparkType(function.getReturnDataType, true)
        }
      case "COUNT" => LongType
      case x if x.startsWith("TOP_N") =>
        val fields = function.getParameters.asScala.drop(1).map(p =>
          StructField(s"DIMENSION_${p.getColRef.getName}", SparderTypeUtil.toSparkType(p.getColRef.getType))
        )
        DataTypes.createArrayType(StructType(Seq(
          StructField("measure", DoubleType),
          StructField("dim", StructType(fields))
        )))
      case "MAX" | "MIN" =>
        val parameter = function.getParameters.get(0)
        if (parameter.isColumnType) {
          SparderTypeUtil.toSparkType(parameter.getColRef.getType)
        } else {
          SparderTypeUtil.toSparkType(function.getReturnDataType)
        }
      case "COLLECT_SET" =>
        val parameter = function.getParameters.get(0)
        ArrayType(SparderTypeUtil.toSparkType(parameter.getColRef.getType))
      case _ => SparderTypeUtil.toSparkType(function.getReturnDataType)
    }
  }
}
