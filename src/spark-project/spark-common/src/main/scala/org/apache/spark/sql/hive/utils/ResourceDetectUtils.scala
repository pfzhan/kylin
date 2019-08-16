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

package org.apache.spark.sql.hive.utils

import java.util.{List => JList, Map => JMap}
import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import io.kyligence.kap.metadata.cube.model.LayoutEntity
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, FSDataOutputStream, Path}
import org.apache.kylin.common.util.{HadoopUtil, JsonUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

import scala.collection.JavaConverters._

object ResourceDetectUtils extends Logging {
  def getPaths(plan: SparkPlan): Seq[Path] = {
    var paths = Seq.empty[Path]
    plan.foreach {
      case plan: FileSourceScanExec =>
        paths ++= plan.relation.location.rootPaths
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
      case _ =>
    }
    paths.distinct
  }

  def findCountDistinctMeasure(layouts: util.Collection[LayoutEntity]): Boolean = {
    for (layoutEntity <- layouts.asScala) {
      for (measure <- layoutEntity.getOrderedMeasures.values.asScala) {
        if (measure.getFunction.getExpression.equalsIgnoreCase("COUNT_DISTINCT"))  return true
      }
    }
    false
  }

  def getResourceSize(paths: Path*): Long = {
    val fs = HadoopUtil.getFileSystem(paths.head)
    paths.map(HadoopUtil.getContentSummary(fs, _).getLength).sum
  }

  def getMaxResourceSize(resourcePaths: JMap[String, JList[String]]): Long = {
    resourcePaths.values.asScala.map(value => getResourceSize(value.asScala.map(path => new Path(path)): _*)).max
  }

  def write(path: Path, item: Object): Unit = {
    val fs = HadoopUtil.getFileSystem(path)
    var out: FSDataOutputStream = null
    try {
      out = fs.create(path)
      out.writeUTF(JsonUtil.writeValueAsString(item))
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  def selectMaxValueInFiles(files: Array[FileStatus]): String = {
    files.map(f => readResourcePathsAs[JMap[String, Integer]](f.getPath).values().asScala.max).max.toString
  }
  

  def readDetectItems(path: Path): JMap[EnumDetectItem, String] = {
    log.info(s"Read detectItems from " + path)
    val fs = HadoopUtil.getFileSystem(path)
    val typeRef = new TypeReference[JMap[EnumDetectItem, String]]() {}
    var in: FSDataInputStream = null
    try {
      in = fs.open(path)
      JsonUtil.readValue(in.readUTF, typeRef)
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }

  def readResourcePathsAs[T](path: Path): T = {
    log.info(s"Read resource paths form $path")
    val fs = HadoopUtil.getFileSystem(path)
    val typeRef = new TypeReference[T]() {}
    var in: FSDataInputStream = null
    try {
      in = fs.open(path)
      JsonUtil.readValue(in.readUTF, typeRef)
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