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

package org.apache.spark.sql.datasource.storage

import java.util
import java.util.{List => JList}

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.engine.spark.utils.StorageUtils.findCountDistinctMeasure
import io.kyligence.kap.engine.spark.utils.{Metrics, Repartitioner, StorageUtils}
import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataSegment, NDataflow}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KapConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.LayoutEntityConverter._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasource.FilePruner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.DetectDataSkewException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class WriteTaskStats(
                           numPartitions: Int,
                           numFiles: Long,
                           numBytes: Long,
                           numRows: Long,
                           sourceRows: Long,
                           numBucket: Int,
                           partitionValues: JList[String])

abstract class StorageStore extends Logging {

  def save(
            layout: LayoutEntity,
            outputPath: Path,
            kapConfig: KapConfig,
            dataFrame: DataFrame): WriteTaskStats

  def read(
    dataflow: NDataflow, layout: LayoutEntity, sparkSession: SparkSession,
    extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame

  def readSpecialSegment(
    segment: NDataSegment, layout: LayoutEntity, sparkSession: SparkSession,
    extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame

  def collectFileCountAndSizeAfterSave(outputPath: Path, conf: Configuration): (Long, Long) = {
    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      val cs = HadoopUtil.getContentSummary(fs, outputPath)
      (cs.getFileCount, cs.getLength)
    } else {
      (0L, 0L)
    }
  }

  def deleteTempPathsWithInfo(conf: Configuration, tempPaths: String*): Unit = {
    if (tempPaths.nonEmpty) {
      val paths = tempPaths.map(path => new Path(path))
      val fs = paths.head.getFileSystem(conf)
      paths.foreach { path =>
        if (fs.exists(path)) {
          if (fs.delete(path, true)) {
            logInfo(s"Delete temp layout path successful. Temp path: $path.")
          } else {
            logError(s"Delete temp layout path wrong, leave garbage. Temp path: $path.")
          }
        }
      }
    }
  }
}

class StorageStoreV1 extends StorageStore {
  override def save(layout: LayoutEntity, outputPath: Path, kapConfig: KapConfig, dataFrame: DataFrame): WriteTaskStats = {
    val tempPath = outputPath.toString + "_temp1"
    val metrics = StorageUtils.writeWithMetrics(dataFrame, tempPath)
    val rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT)
    val hadoopConf = dataFrame.sparkSession.sparkContext.hadoopConfiguration

    val bucketNum = StorageUtils.calculateBucketNum(tempPath, layout, rowCount, kapConfig)
    val summary = HadoopUtil.getContentSummary(outputPath.getFileSystem(hadoopConf), new Path(tempPath))
    val repartitionThresholdSize = if (findCountDistinctMeasure(layout)) {
      kapConfig.getParquetStorageCountDistinctShardSizeRowCount
    } else {
      kapConfig.getParquetStorageShardSizeRowCount
    }
    val repartitioner = new Repartitioner(
      kapConfig.getParquetStorageShardSizeMB,
      kapConfig.getParquetStorageRepartitionThresholdSize,
      rowCount,
      repartitionThresholdSize,
      summary,
      layout.getShardByColumns,
      layout.getOrderedDimensions.keySet().asList()
    )
    repartitioner.doRepartition(outputPath.toString, tempPath, bucketNum, dataFrame.sparkSession)

    val (fileCount, byteSize) = collectFileCountAndSizeAfterSave(outputPath, hadoopConf)
    WriteTaskStats(0, fileCount, byteSize, rowCount, metrics.getMetrics(Metrics.SOURCE_ROWS_CNT), bucketNum, new util.ArrayList[String]())
  }

  override def read(dataflow: NDataflow, layout: LayoutEntity, sparkSession: SparkSession,
                    extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame = {
    val indexCatalog = new FilePruner(sparkSession, options = extraOptions, layout.toSchema())
    sparkSession.baseRelationToDataFrame(
      HadoopFsRelation(
        indexCatalog,
        partitionSchema = indexCatalog.partitionSchema,
        dataSchema = indexCatalog.dataSchema.asNullable,
        bucketSpec = None,
        new ParquetFileFormat,
        options = extraOptions)(sparkSession))
  }

  override def readSpecialSegment(
    segment: NDataSegment, layout: LayoutEntity, sparkSession: SparkSession,
    extraOptions: Map[String, String]): DataFrame = {
    val layoutId = layout.getId
    val path = NSparkCubingUtil.getStoragePath(segment, layoutId)
    sparkSession.read.parquet(path)
  }
}

class StorageStoreV2 extends StorageStore with Logging {

  override def save(layout: LayoutEntity, outputPath: Path, kapConfig: KapConfig, dataFrame: DataFrame): WriteTaskStats = {
    val outputPathStr = outputPath.toString
    val tempPath1 = outputPathStr + "_temp1"
    val tempPath2 = outputPathStr + "_temp2"
    val sparkSession = dataFrame.sparkSession
    val metrics = StorageUtils.writeWithMetrics(dataFrame, tempPath1)
    val rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT)
    val bucket = StorageUtils.calculateBucketNum(tempPath1, layout, rowCount, kapConfig)
    val bucketNum = if (!layout.getIndex.getIndexPlan.getLayoutBucketNumMapping.containsKey(layout.getId)) {
      val num = Math.max(bucket, kapConfig.getMinBucketsNumber)
      layout.getIndex.getIndexPlan.getLayoutBucketNumMapping.put(layout.getId, num)
      num
    } else {
      layout.getIndex.getIndexPlan.getLayoutBucketNumMapping.get(layout.getId).toInt
    }
    val table = layout.toCatalogTable()
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val fs = outputPath.getFileSystem(hadoopConf)
    val tmpDF = sparkSession.read.parquet(tempPath1)

    val partitionValues = if (table.bucketSpec.isDefined || table.partitionColumnNames.nonEmpty) {
      // write temp data to tempPath2 with bucketBy column and partitionBy column
      val repartitionColumns = {
        if (table.bucketSpec.isDefined) {
          table.partitionColumnNames ++ table.bucketSpec.get.bucketColumnNames
        } else {
          table.partitionColumnNames
        }
        }.map(col)

      require(repartitionColumns.nonEmpty, "Repartition columns should not be empty. Check your partition column and bucket column.")
      val afterRepartition = tmpDF.repartition(bucketNum, repartitionColumns: _*)
      val partitionDirs = {
        try {
          writeBucketAndPartitionFile(afterRepartition, table, hadoopConf, outputPath)
        } catch {
          case e: SparkException =>
            e.getCause.getCause match {
              case exception: DetectDataSkewException =>
                logWarning("Case DetectDataSkewException, switch to build skew data and no skew data separately")
                val bucketIds = exception.getBucketIds
                val skewData = tmpDF
                  .filter(Column(HashPartitioning(repartitionColumns.map(_.expr), bucketNum).partitionIdExpression).isin(bucketIds: _*))

                val noSkewData = tmpDF
                  .filter(not(
                    Column(HashPartitioning(repartitionColumns.map(_.expr), bucketNum).partitionIdExpression).isin(bucketIds: _*)
                  )).repartition(bucketNum, repartitionColumns: _*)

                // write skew data to temp path2
                val skewPartitions = writeBucketAndPartitionFile(skewData, table, hadoopConf, new Path(tempPath2))
                // write no skew data to output path
                val noSkewPartitions = writeBucketAndPartitionFile(noSkewData, table, hadoopConf, outputPath)
                val partitions = skewPartitions ++ noSkewPartitions

                // move skew data to output path
                if (skewPartitions.isEmpty) {
                  fs.listStatus(new Path(tempPath2)).foreach { file =>
                    StorageUtils.overwriteWithMessage(fs, file.getPath, new Path(s"$outputPath/${file.getPath.getName}"))
                  }
                } else {
                  skewPartitions.foreach { partition =>
                    fs.listStatus(new Path(s"$tempPath2/$partition")).foreach { file =>
                      StorageUtils.overwriteWithMessage(fs, file.getPath, new Path(s"$outputPath/$partition/${file.getPath.getName}"))
                    }
                  }
                }
                partitions
              case e => throw e
            }
        }
      }
      partitionDirs
    } else {
      tmpDF.repartition(bucketNum)
        .sortWithinPartitions(layout.getColOrder.asScala.map(id => col(id.toString)): _*)
        .write
        .mode("overwrite")
        .parquet(outputPathStr)
      Nil
    }

    val (fileCount, byteSize) = collectFileCountAndSizeAfterSave(outputPath, hadoopConf)
    deleteTempPathsWithInfo(hadoopConf, tempPath1, tempPath2)
    WriteTaskStats(partitionValues.size, fileCount, byteSize, rowCount,
      metrics.getMetrics(Metrics.SOURCE_ROWS_CNT), bucketNum, partitionValues.toList.asJava)
  }

  private def writeBucketAndPartitionFile(
                                           dataFrame: DataFrame, table: CatalogTable, hadoopConf: Configuration,
                                           qualifiedOutputPath: Path): Set[String] = {
    val fs = qualifiedOutputPath.getFileSystem(hadoopConf)
    if (fs.exists(qualifiedOutputPath)) {
      logInfo(s"Path $qualifiedOutputPath is exists, delete it.")
      fs.delete(qualifiedOutputPath, true)
    }

    qualifiedOutputPath.getFileSystem(hadoopConf).deleteOnExit(qualifiedOutputPath)
    dataFrame.sparkSession.sessionState.conf.setLocalProperty("spark.sql.adaptive.enabled.when.repartition", "true")
    val partitionDirs = mutable.Set.empty[String]
    runCommand(dataFrame.sparkSession, "UnsafelySave") {
      UnsafelyInsertIntoHadoopFsRelationCommand(qualifiedOutputPath, dataFrame.logicalPlan, table, partitionDirs)
    }
    dataFrame.sparkSession.sessionState.conf.setLocalProperty("spark.sql.adaptive.enabled.when.repartition", null)
    partitionDirs.toSet
  }

  override def read(
    dataflow: NDataflow, layout: LayoutEntity, sparkSession: SparkSession,
    extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame = {
    read(dataflow.getQueryableSegments.asScala, layout, sparkSession, extraOptions)

  }

  override def readSpecialSegment(
    segment: NDataSegment, layout: LayoutEntity, sparkSession: SparkSession,
    extraOptions: Map[String, String]): DataFrame = {
    sparkSession.read.schema(layout.toCatalogTable().schema).parquet(NSparkCubingUtil.getStoragePath(segment, layout.getId))
  }

  def read(
    segments: Seq[NDataSegment], layout: LayoutEntity, sparkSession: SparkSession,
    extraOptions: Map[String, String]): DataFrame = {
    val table = layout.toCatalogTable()
    sparkSession.baseRelationToDataFrame(HadoopFsRelation(
      new LayoutFileIndex(sparkSession, table, 0L, segments),
      partitionSchema = table.partitionSchema,
      dataSchema = table.dataSchema,
      bucketSpec = table.bucketSpec,
      new ParquetFileFormat,
      extraOptions)(sparkSession))
      .select(layout.toSchema().map(tp => col(tp.name)): _*)
  }

  private def runCommand(session: SparkSession, name: String)(command: LogicalPlan): Unit = {
    val qe = session.sessionState.executePlan(command)
    try {
      val start = System.nanoTime()
      // call `QueryExecution.toRDD` to trigger the execution of commands.
      SQLExecution.withNewExecutionId(session, qe)(qe.toRdd)
      val end = System.nanoTime()
      session.listenerManager.onSuccess(name, qe, end - start)
    } catch {
      case e: Exception =>
        session.listenerManager.onFailure(name, qe, e)
        throw e
    }
  }
}
object StorageStoreUtils {

  def toDF(segment: NDataSegment, layoutEntity: LayoutEntity, sparkSession: SparkSession): DataFrame = {
    StorageStoreFactory.create(layoutEntity.getModel.getStorageType)
      .readSpecialSegment(segment, layoutEntity, sparkSession);
  }

}



