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
import java.util.concurrent.Executors
import java.util.{List => JList}

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.engine.spark.utils.StorageUtils.findCountDistinctMeasure
import io.kyligence.kap.engine.spark.utils.{JobMetrics, Metrics, Repartitioner, StorageUtils}
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
import org.apache.spark.sql.execution.datasource.{FilePruner, LayoutFileIndex}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.DetectDataSkewException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.util.ThreadUtils

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

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
}

class StorageStoreV1 extends StorageStore {
  override def save(layout: LayoutEntity, outputPath: Path, kapConfig: KapConfig, dataFrame: DataFrame): WriteTaskStats = {
    val (metrics: JobMetrics, rowCount: Long, hadoopConf: Configuration, bucketNum: Int) =
      repartitionWriter(layout, outputPath, kapConfig, dataFrame)
    val (fileCount, byteSize) = collectFileCountAndSizeAfterSave(outputPath, hadoopConf)
    checkAndWriterFastBitmapLayout(dataFrame, layout, kapConfig, outputPath)
    WriteTaskStats(0, fileCount, byteSize, rowCount, metrics.getMetrics(Metrics.SOURCE_ROWS_CNT), bucketNum, new util.ArrayList[String]())
  }

  private def repartitionWriter(layout: LayoutEntity, outputPath: Path, kapConfig: KapConfig, dataFrame: DataFrame) = {
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
    (metrics, rowCount, hadoopConf, bucketNum)
  }

  def checkAndWriterFastBitmapLayout(dataset: DataFrame, layoutEntity: LayoutEntity, kapConfig: KapConfig, layoutPath: Path): Unit = {
    if (!layoutEntity.getIndex.getIndexPlan.isFastBitmapEnabled) {
      return
    }
    val bitmaps = layoutEntity.listBitmapMeasure()
    if (bitmaps.isEmpty) {
      return
    }
    logInfo(s"Begin write fast bitmap cuboid. layout id is ${layoutEntity.getId}")
    val outputPath = new Path(layoutPath.toString + HadoopUtil.FAST_BITMAP_SUFFIX)

    def replaceCountDistinctEvalColumn(list: java.util.List[String], dataFrame: DataFrame): DataFrame = {
      val columns = dataFrame.schema.names.map(name =>
        if (list.contains(name)) {
          callUDF("eval_bitmap", col(name)).as(name)
        } else {
          col(name)
        })
      dataFrame.select(columns: _*)
    }
    val afterReplaced = replaceCountDistinctEvalColumn(bitmaps, dataset)
    repartitionWriter(layoutEntity, outputPath, kapConfig, afterReplaced)
  }


  override def read(dataflow: NDataflow, layout: LayoutEntity, sparkSession: SparkSession,
                    extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame = {
    val structType = if ("true".equals(extraOptions.apply("isFastBitmapEnabled"))) {
      layout.toExactlySchema()
    } else {
       layout.toSchema()
    }
    val indexCatalog = new FilePruner(sparkSession, options = extraOptions, structType)
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
    val sparkSession = dataFrame.sparkSession
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val fs = outputPath.getFileSystem(hadoopConf)
    StorageUtils.cleanTempPath(fs, outputPath, cleanSelf = true)
    val tempPath1 = outputPathStr + "_temp1"
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

    val tmpDF = sparkSession.read.parquet(tempPath1)
    val (normalCase, skewCase) = StorageStoreUtils.extractRepartitionColumns(table, layout)

    val partitionValues = if (normalCase.nonEmpty) {
      val afterRepartition = tmpDF.repartition(bucketNum, normalCase: _*)
      val partitionDirs = {
        try {
          StorageStoreUtils.writeBucketAndPartitionFile(afterRepartition, table, hadoopConf, outputPath)
        } catch {
          case e: SparkException =>
            e.getCause.getCause match {
              case exception: DetectDataSkewException =>
                logWarning("Case DetectDataSkewException, switch to resolve data skew.")
                StorageStoreUtils.writeSkewData(exception.getBucketIds, tmpDF, outputPath, table, normalCase, skewCase, bucketNum)
              case _ => throw e
            }
        }
      }
      partitionDirs
    } else {
      tmpDF.repartition(bucketNum)
        .sortWithinPartitions(layout.getColOrder.asScala.map(id => col(id.toString)): _*)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(outputPathStr)
      Nil
    }

    val (fileCount, byteSize) = collectFileCountAndSizeAfterSave(outputPath, hadoopConf)
    StorageUtils.cleanTempPath(fs, outputPath, cleanSelf = false)
    WriteTaskStats(partitionValues.size, fileCount, byteSize, rowCount,
      metrics.getMetrics(Metrics.SOURCE_ROWS_CNT), bucketNum, partitionValues.toList.asJava)
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
}

object StorageStoreUtils extends Logging{
  def writeSkewData(bucketIds: Seq[Int], dataFrame: DataFrame, outputPath: Path, table: CatalogTable,
                      normalCase: Seq[Column], skewCase: Seq[Column], bucketNum: Int
                     ): Set[String] = {
    withNoSkewDetectScope(dataFrame.sparkSession) {
      val hadoopConf = dataFrame.sparkSession.sparkContext.hadoopConfiguration
      val fs = outputPath.getFileSystem(hadoopConf)
      val outputPathStr = outputPath.toString

      // filter out skew data, then write it separately
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = {
        bucketIds.map { bucketId =>
          (dataFrame
            .filter(Column(HashPartitioning(normalCase.map(_.expr), bucketNum).partitionIdExpression) === bucketId)
            .repartition(bucketNum, skewCase: _*), new Path(outputPathStr + s"_temp_$bucketId"))
        } :+ {
          (dataFrame
            .filter(not(
              Column(HashPartitioning(normalCase.map(_.expr), bucketNum).partitionIdExpression).isin(bucketIds: _*)
            )).repartition(bucketNum, normalCase: _*), outputPath)
        }
        }.map { case (df, path) =>
        Future[(Path, Set[String])] {
          try {
            val partitionDirs =
              StorageStoreUtils.writeBucketAndPartitionFile(df, table, hadoopConf, path)
            (path, partitionDirs)
          } catch {
            case t: Throwable =>
              logError(s"Error for write skew data concurrently.", t)
              throw t
          }
        }
      }

      val results = try {
        val eventualFuture = Future.sequence(futures.toList)
        ThreadUtils.awaitResult(eventualFuture, Duration.Inf)
      } catch {
        case t: Throwable =>
          ThreadUtils.shutdown(service)
          throw t
      }

      // move skew data to final output path
      if (table.partitionColumnNames.isEmpty) {
        results.map(_._1).filter(!_.toString.equals(outputPathStr)).foreach { path =>
          fs.listStatus(path).foreach { file =>
            StorageUtils.overwriteWithMessage(fs, file.getPath, new Path(s"$outputPath/${file.getPath.getName}"))
          }
        }
      } else {
        logInfo(s"with partition column, results $results")
        results.filter(!_._1.toString.equals(outputPathStr))
          .foreach { case (path: Path, partitions: Set[String]) =>
            partitions.foreach { partition =>
              if (!fs.exists(new Path(s"$outputPath/$partition"))) {
                fs.mkdirs(new Path(s"$outputPath/$partition"))
              }
              fs.listStatus(new Path(s"$path/$partition")).foreach { file =>
                StorageUtils.overwriteWithMessage(fs, file.getPath, new Path(s"$outputPath/$partition/${file.getPath.getName}"))
              }
            }
          case _ => throw new RuntimeException
          }
      }
      results.flatMap(_._2).toSet
    }
  }

  def extractRepartitionColumns(table: CatalogTable, layout: LayoutEntity): (Seq[Column], Seq[Column]) = {
    (table.bucketSpec.isDefined, table.partitionColumnNames.nonEmpty) match {
      case (true, true) => (table.bucketSpec.get.bucketColumnNames.map(col), table.partitionColumnNames.map(col))
      case (false, true) => (table.partitionColumnNames.map(col), layout.getColOrder.asScala.map(id => col(id.toString)))
      case (true, false) => (table.bucketSpec.get.bucketColumnNames.map(col), layout.getColOrder.asScala.map(id => col(id.toString)))
      case (false, false) => (Seq.empty[Column], Seq.empty[Column])
    }
  }

  private def withNoSkewDetectScope[U](ss: SparkSession) (body: => U): U = {
    try {
      ss.sessionState.conf.setLocalProperty("spark.sql.adaptive.shuffle.maxTargetPostShuffleInputSize", "-1")
      body
    } catch {
      case e: Throwable => throw e
    }
    finally {
      ss.sessionState.conf.setLocalProperty("spark.sql.adaptive.shuffle.maxTargetPostShuffleInputSize", null)
    }
  }

  def toDF(segment: NDataSegment, layoutEntity: LayoutEntity, sparkSession: SparkSession): DataFrame = {
    StorageStoreFactory.create(layoutEntity.getModel.getStorageType).readSpecialSegment(segment, layoutEntity, sparkSession)
  }

  def writeBucketAndPartitionFile(
                                   dataFrame: DataFrame, table: CatalogTable, hadoopConf: Configuration,
                                   qualifiedOutputPath: Path): Set[String] = {
    dataFrame.sparkSession.sessionState.conf.setLocalProperty("spark.sql.adaptive.enabled.when.repartition", "true")
    var partitionDirs = Set.empty[String]
    runCommand(dataFrame.sparkSession, "UnsafelySave") {
      UnsafelyInsertIntoHadoopFsRelationCommand(qualifiedOutputPath, dataFrame.logicalPlan, table,
        set => partitionDirs = partitionDirs ++ set)
    }
    dataFrame.sparkSession.sessionState.conf.setLocalProperty("spark.sql.adaptive.enabled.when.repartition", null)
    partitionDirs
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



