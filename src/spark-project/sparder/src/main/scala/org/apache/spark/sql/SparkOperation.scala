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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.GroupingSets
import org.apache.spark.sql.types.StructType

object SparkOperation {
  //    def createDictPushDownTable(args: CreateDictPushdownTableArgc): DataFrame = {
  //      val spark = SparderEnv.getSparkSession
  //      spark.read
  //        .format(args.fileFormat)
  //        .option(SparderConstants.PARQUET_FILE_FILE_TYPE, SparderConstants.PARQUET_FILE_CUBE_TYPE)
  //        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, args.gtinfo)
  //  //      .option(
  //  //        ParquetFormatConstants.KYLIN_BUNDLE_READER,
  //  //        KapConfig.getInstanceFromEnv.getBundleReader)
  //        .option(SparderConstants.CUBOID_ID, args.cuboid)
  //        .option(SparderConstants.DICT_PATCH, args.dictPatch)
  //        .option(SparderConstants.DICT, args.dict)
  //        .option(SparderConstants.TABLE_ALIAS, args.tableName)
  //        .option(SparderConstants.STORAGE_TYPE, args.storageType)
  //        .schema(args.schema)
  //        .option(SparderConstants.PAGE_FILTER_PUSH_DOWN, args.pushdown)
  //        .option(SparderConstants.BINARY_FILTER_PUSH_DOWN, args.binaryFilterPushdown)
  //        .option("segmentId", args.uuid)
  //        .option(SparderConstants.DIAGNOSIS_WRITER_TYPE, args.diagnosisWriterType)
  //        .load(args.filePath: _*)
  //        .as(args.tableName)
  //    }

  def createEmptyDataFrame(structType: StructType): DataFrame = {
    SparderEnv.getSparkSession
      .createDataFrame(new java.util.ArrayList[Row], structType)
  }

  def createEmptyRDD(): RDD[InternalRow] = {
    SparderEnv.getSparkSession.sparkContext.emptyRDD[InternalRow]
  }

  def createConstantDataFrame(rows: java.util.List[Row], structType: StructType): DataFrame = {
    SparderEnv.getSparkSession.createDataFrame(rows, structType)
  }

  def agg(aggArgc: AggArgc): DataFrame = {
    if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty && !aggArgc.isSimpleGroup && aggArgc.groupSets.nonEmpty) {
      Dataset.ofRows(
        aggArgc.dataFrame.sparkSession,
        GroupingSets(
          aggArgc.groupSets.map(gs => gs.map(_.expr)),
          aggArgc.group.map(_.expr),
          aggArgc.dataFrame.queryExecution.logical,
          aggArgc.group.map(_.named) ++ aggArgc.agg.map(_.named)
        )
      )
    } else if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame
        .groupBy(aggArgc.group: _*)
        .agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else if (aggArgc.agg.isEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame.dropDuplicates(aggArgc.group.map(_.toString()))
    } else if (aggArgc.agg.nonEmpty && aggArgc.group.isEmpty) {
      aggArgc.dataFrame.agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else {
      aggArgc.dataFrame
    }
  }

  /*
    /**
      * Collect all elements from a spark plan.
      */
    private def collectFromPlan(plan: SparkPlan, deserializer: Expression): Array[Row] = {
      // This projection writes output to a `InternalRow`, which means applying this projection is not
      // thread-safe. Here we create the projection inside this method to make `Dataset` thread-safe.
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      plan.executeCollect().map { row =>
        // The row returned by SafeProjection is `SpecificInternalRow`, which ignore the data type
        // parameter of its `get` method, so it's safe to use null here.
        objProj(row).get(0, null).asInstanceOf[Row]
      }
    }
  */
}

case class AggArgc(dataFrame: DataFrame, group: List[Column], agg: List[Column], groupSets: List[List[Column]], isSimpleGroup: Boolean)