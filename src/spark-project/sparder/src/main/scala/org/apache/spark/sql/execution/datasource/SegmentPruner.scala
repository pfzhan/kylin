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

package org.apache.spark.sql.execution.datasource

import java.sql.{Date, Timestamp}

import io.kyligence.kap.metadata.cube.model.{NDataflow, NDataflowManager}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.PartitionDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class SegmentPruner(val session: SparkSession,
                    val options: Map[String, String],
                    userSpecifiedSchema: Option[StructType])
  extends FileIndex with Logging {

  private var timePartitionColumn: Attribute = _

  private val cuboidId: Long = options.getOrElse("cuboidId", sys.error("cuboidId option is required")).toLong

  private val dataflow: NDataflow = {
    val dataflowId = options.getOrElse("dataflowId", sys.error("dataflowId option is required"))
    val prj = options.getOrElse("project", sys.error("project option is required"))
    val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj)
    dfMgr.getDataflow(dataflowId)
  }

  override def rootPaths: Seq[Path] = {
    val segments = dataflow.getQueryableSegments
    val basePath = KapConfig.wrap(dataflow.getConfig).getReadParquetStoragePath(dataflow.getProject)
    segments.asScala.map(
      seg => new Path(s"$basePath${dataflow.getUuid}/${seg.getId}/$cuboidId")
    )
  }

  lazy val catalog = new InMemoryFileIndex(session, rootPaths, options, userSpecifiedSchema, FileStatusCache.getOrCreate(session))

  override lazy val partitionSchema: StructType = {
    // we not use this.
    new StructType()
  }

  lazy val dataSchema: StructType = {
    if (userSpecifiedSchema.isDefined) {
      userSpecifiedSchema.get
    } else {
      new ParquetFileFormat().inferSchema(session, options, catalog.allFiles()).get
    }
  }

  lazy val indexSchema: StructType = {
    val desc: PartitionDesc = dataflow.getModel.getPartitionDesc
    StructType(
      if (desc != null) {
        val layout = dataflow.getIndexPlan.getCuboidLayout(cuboidId)
        // only consider partition date column
        // we can only get col ID in layout cuz data schema is all ids.
        val id = layout.getOrderedDimensions.inverse().get(desc.getPartitionDateColumnRef)
        if (id != null) {
          dataSchema.filter(_.name == id.toString)
        } else {
          Seq.empty
        }
      } else {
        Seq.empty
      })
  }

  def resolve(relation: LogicalRelation, resolver: Resolver): Unit = {
    val attributes = relation.resolve(indexSchema, resolver)
    if (attributes.nonEmpty) {
      timePartitionColumn = attributes.head
    }
  }

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    val timePartitionFilters = dataFilters
      .filter(_.references.subsetOf(AttributeSet(timePartitionColumn)))
      .flatMap(DataSourceStrategy.translateFilter)

    logInfo(s"Applying time partition filters: ${timePartitionFilters.mkString(",")}")

    val allPartitions = catalog.listFiles(Nil, Nil)
    val filteredPartitions = if (timePartitionFilters.isEmpty) {
      allPartitions
    } else {
      var selectedSegments = allPartitions
      try {
        val startTime = System.nanoTime
        selectedSegments = pruneSegments(timePartitionFilters, allPartitions)
        val endTime = System.nanoTime()
        logInfo(s"Filtered segments in ${(endTime - startTime).toDouble / 1000000} ms")
      } catch {
        case th: Throwable =>
          logError(s"Error occurs when pruning segments, scan all segments.", th)
      }
      selectedSegments
    }

    logInfo("Selected files after segment pruning:\n\t" + filteredPartitions.flatMap(_.files).mkString("\n\t"))

    filteredPartitions
  }

  private def pruneSegments(filters: Seq[Filter],
                            partitions: Seq[PartitionDirectory]): Seq[PartitionDirectory] = {
    val allFiles = partitions.flatMap(_.files)

    val segWithRange = allFiles.zip(allFiles.map(f => {
      // first parent is cuboid, next parent is segment.
      val segId = f.getPath.getParent.getParent.getName

      // current we only support ts range.
      dataflow.getSegment(segId).getTSRange
    }))

    // reduce filters to supported only
    val reducedFilter = filters.reduceLeft(And)

    val filteredStatuses = segWithRange.flatMap {
      pair => {
        val tsRange = pair._2
        SegFilters(tsRange.getStart, tsRange.getEnd).foldFilter(reducedFilter) match {
          case Trivial(true) => Some(pair._1)
          case Trivial(false) => None
        }
      }
    }

    if (filteredStatuses.isEmpty) {
      Seq.empty
    } else {
      Seq(PartitionDirectory(InternalRow.empty, filteredStatuses))
    }
  }

  override lazy val inputFiles: Array[String] = catalog.listFiles(Nil, Nil).flatMap { partition =>
    partition.files.map(_.getPath.toString)
  }.toArray

  override lazy val sizeInBytes: Long = catalog.listFiles(Nil, Nil).flatMap { partition =>
    partition.files.map(_.getLen)
  }.sum

  override def refresh(): Unit = {}
}

case class SegFilters(start: Long, end: Long) extends Logging {

  private def insurance(value: Any)
                       (func: Long => Filter): Filter = {
    value match {
      // current we only support date/time range.
      case v: Date =>
        // see SPARK-27546
        val ts = DateFormat.stringToMillis(v.toString)
        func(ts)
      case v: Timestamp =>
        func(v.getTime)
      case _ =>
        Trivial(true)
    }
  }

  /**
   * Recursively fold provided filters to trivial,
   * blocks are always non-empty.
   */
  def foldFilter(filter: Filter): Filter = {
    filter match {
      case EqualTo(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts >= start && ts < end)
        }
      case In(_, values: Array[Any]) =>
        val satisfied = values.map(v => insurance(v) {
          ts => Trivial(ts >= start && ts < end)
        }).exists(_.equals(Trivial(true)))
        Trivial(satisfied)

      case IsNull(_) =>
        Trivial(false)
      case IsNotNull(_) =>
        Trivial(true)
      case GreaterThan(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts < end)
        }
      case GreaterThanOrEqual(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts < end)
        }
      case LessThan(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts > start)
        }
      case LessThanOrEqual(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts >= start)
        }
      case And(left: Filter, right: Filter) =>
        And(foldFilter(left), foldFilter(right)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldFilter(left), foldFilter(right)) match {
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case other => other
        }
      case trivial: Trivial =>
        trivial
      case unsupportedFilter =>
        // return 'true' to scan all partitions
        // currently unsupported filters are:
        // - StringStartsWith
        // - StringEndsWith
        // - StringContains
        // - EqualNullSafe
        Trivial(true)
    }
  }
}