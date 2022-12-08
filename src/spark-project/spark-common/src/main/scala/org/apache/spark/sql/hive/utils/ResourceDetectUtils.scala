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

package org.apache.spark.sql.hive.utils

import java.io.IOException
import java.nio.charset.Charset
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicLong
import java.util.{Map => JMap}

import com.google.common.collect.Maps
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.hadoop.conf.Configuration
import org.apache.kylin.metadata.cube.model.{DimensionRangeInfo, LayoutEntity}
import org.apache.hadoop.fs._
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.sources.NBaseRelation

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport

object ResourceDetectUtils extends Logging {
  private val json = new Gson()

  def getPaths(plan: SparkPlan): Seq[Path] = {
    var paths = Seq.empty[Path]
    plan.foreach {
      case plan: FileSourceScanExec =>
        if (plan.relation.location.partitionSchema.nonEmpty) {
          val selectedPartitions = plan.relation.location.listFiles(plan.partitionFilters, plan.dataFilters)
          selectedPartitions.flatMap(partition => partition.files).foreach(file => {
            paths :+= file.getPath
          })
        } else {
          paths ++= plan.relation.location.rootPaths
        }
      case plan: LayoutFileSourceScanExec =>
        if (plan.relation.location.partitionSchema.nonEmpty) {
          val selectedPartitions = plan.relation.location.listFiles(plan.partitionFilters, plan.dataFilters)
          selectedPartitions.flatMap(partition => partition.files).foreach(file => {
            paths :+= file.getPath
          })
        } else {
          paths ++= plan.relation.location.rootPaths
        }
      case plan: InMemoryTableScanExec =>
        val _plan = plan.relation.cachedPlan
        paths ++= getPaths(_plan)
      case plan: HiveTableScanExec =>
        if (plan.relation.isPartitioned) {
          plan.rawPartitions.foreach { partition =>
            paths ++= partition.getPath
          }
        } else {
          paths :+= new Path(plan.relation.tableMeta.location)
        }
      case plan: RowDataSourceScanExec =>
        plan.relation match {
          case relation: NBaseRelation =>
            paths :+= relation.location
          case _ =>
        }
      case _ =>
    }
    paths
  }

  def getPartitions(plan: SparkPlan): String = {
    val leafNodePartitionsLengthMap: mutable.Map[String, Int] = mutable.Map()
    var pNum = 0
    plan.foreach {
      case node: LeafExecNode =>
        val pn = node match {
          case ree: ReusedExchangeExec if ree.child.isInstanceOf[BroadcastExchangeExec] => 1
          case _ => leafNodePartitionsLengthMap.getOrElseUpdate(node.nodeName, node.execute().partitions.length)
        }
        pNum = pNum + pn
        logInfo(s"${node.nodeName} partition size $pn")
      case _ =>
    }
    logInfo(s"Partition num $pNum")
    pNum.toString
  }

  @throws[IOException]
  protected def listSourcePath(shareDir: Path): java.util.Map[String, java.util.Map[String, Double]] = {
    val fs = HadoopUtil.getWorkingFileSystem
    val resourcePaths = Maps.newHashMap[String, java.util.Map[String, Double]]()
    if (fs.exists(shareDir)) {
      val fileStatuses = fs.listStatus(shareDir, new PathFilter {
        override def accept(path: Path): Boolean = {
          path.toString.endsWith(ResourceDetectUtils.fileName())
        }
      })
      for (file <- fileStatuses) {
        val fileName = file.getPath.getName
        val segmentId = fileName.substring(0, fileName.indexOf(ResourceDetectUtils.fileName) - 1)
        val map = ResourceDetectUtils.readResourcePathsAs[java.util.Map[String, Double]](file.getPath)
        resourcePaths.put(segmentId, map)
      }
    }
    // return size with unit
    resourcePaths
  }

  def findCountDistinctMeasure(layouts: java.util.Collection[LayoutEntity]): Boolean = {
    for (layoutEntity <- layouts.asScala) {
      for (measure <- layoutEntity.getOrderedMeasures.values.asScala) {
        if (measure.getFunction.getExpression.equalsIgnoreCase("COUNT_DISTINCT")) return true
      }
    }
    false
  }

  def getResourceSizeConcurrency(configuration: Configuration, paths: Path*): Long = {
    val kylinConfig = KylinConfig.getInstanceFromEnv
    val threadNumber = kylinConfig.getConcurrencyFetchDataSourceSizeThreadNumber
    logInfo(s"Get resource size concurrency, thread number is $threadNumber")
    val forkJoinPool = new ForkJoinPool(threadNumber)
    val parallel = paths.par
    val sum = new AtomicLong()
    try {
      parallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
      parallel.foreach {
        path => {
          val fs = path.getFileSystem(configuration)
          if (fs.exists(path)) {
            sum.addAndGet(HadoopUtil.getContentSummaryFromHdfsKylinConfig(fs, path, kylinConfig).getLength)
          }
        }
      }
    }
    finally {
      forkJoinPool.shutdownNow()
    }
    sum.get()
  }


  def getResourceSize(configuration: Configuration, isConcurrencyFetchDataSourceSize: Boolean, paths: Path*): Long = {
    val resourceSize = {
      if (isConcurrencyFetchDataSourceSize) {
        getResourceSizeConcurrency(configuration, paths: _*)
      } else {
        paths.map(path => {
          val fs = path.getFileSystem(configuration)
          if (fs.exists(path)) {
            HadoopUtil.getContentSummary(fs, path).getLength
          } else {
            0L
          }
        }).sum
      }
    }
    resourceSize
  }

  def getResourceSize(isConcurrencyFetchDataSourceSize: Boolean, paths: Path*): Long = {
    getResourceSize(HadoopUtil.getCurrentConfiguration, isConcurrencyFetchDataSourceSize, paths: _*)
  }

  def getMaxResourceSize(shareDir: Path): Long = {
    ResourceDetectUtils.listSourcePath(shareDir)
      .values.asScala
      .flatMap(value => value.values().asScala)
      .max
      .longValue()
  }

  def getSegmentSourceSize(shareDir: Path): java.util.Map[String, Long] = {
    // For a number without fractional part, Gson would convert it as Double,need to convert Double to Long
    ResourceDetectUtils.listSourcePath(shareDir).asScala
      .filter(_._2.keySet().contains("-1"))
      .map(tp => (tp._1, tp._2.get("-1").longValue()))
      .toMap
      .asJava
  }

  def write(path: Path, item: Object): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem()
    var out: FSDataOutputStream = null
    try {
      out = fs.create(path)
      val str = json.toJson(item)
      val bytes = str.getBytes(Charset.defaultCharset)
      out.writeInt(bytes.length)
      out.write(bytes)
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  def selectMaxValueInFiles(files: Array[FileStatus]): String = {
    files.map(f => readResourcePathsAs[JMap[String, Double]](f.getPath).values().asScala.max).max.toString
  }


  def readDetectItems(path: Path): JMap[String, String] = readResourcePathsAs[JMap[String, String]](path)

  def readSegDimRangeInfo(path: Path): java.util.Map[String, DimensionRangeInfo] = {
    if (HadoopUtil.getWorkingFileSystem().exists(path)) {
      readResourcePathsAs[java.util.Map[String, DimensionRangeInfo]](path)
    } else {
      null
    }
  }

  def readResourcePathsAs[T](path: Path): T = {
    log.info(s"Read resource paths form $path")
    val fs = HadoopUtil.getWorkingFileSystem
    var in: FSDataInputStream = null
    try {
      in = fs.open(path)
      val i = in.readInt()
      val bytes = new Array[Byte](i)
      in.readFully(bytes)
      json.fromJson(new String(bytes, Charset.defaultCharset), new TypeToken[T]() {}.getType)
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }


  def fileName(): String = {
    "resource_paths.json"
  }

  val cubingDetectItemFileSuffix: String = "cubing_detect_items.json"

  val samplingDetectItemFileSuffix: String = "sampling_detect_items.json"

  val countDistinctSuffix: String = "count_distinct.json"
}
