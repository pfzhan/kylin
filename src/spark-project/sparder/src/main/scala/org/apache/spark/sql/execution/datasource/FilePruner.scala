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

import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataflow, NDataflowManager}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.PartitionDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, EmptyRow, Expression, Literal}
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import scala.collection.JavaConverters._

case class SegmentDirectory(segmentID: String, files: Seq[FileStatus])

class FilePruner(val session: SparkSession,
                 val options: Map[String, String],
                 val dataSchema: StructType)
  extends FileIndex with Logging {

  private val dataflow: NDataflow = {
    val dataflowId = options.getOrElse("dataflowId", sys.error("dataflowId option is required"))
    val prj = options.getOrElse("project", sys.error("project option is required"))
    val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj)
    dfMgr.getDataflow(dataflowId)
  }

  private val layout: LayoutEntity = {
    val cuboidId = options.getOrElse("cuboidId", sys.error("cuboidId option is required")).toLong
    dataflow.getIndexPlan.getCuboidLayout(cuboidId)
  }

  override def rootPaths: Seq[Path] = {
    val basePath = KapConfig.wrap(dataflow.getConfig).getReadParquetStoragePath(dataflow.getProject)
    dataflow.getQueryableSegments.asScala.map(
      seg => new Path(s"$basePath${dataflow.getUuid}/${seg.getId}/${layout.getId}")
    )
  }

  private lazy val segmentDirs: Seq[SegmentDirectory] = {
    rootPaths.map(path => {
      // path like that: base/dataflowID/segID/layoutID
      val segID = path.getParent.getName

      val files = new InMemoryFileIndex(session,
        path :: Nil,
        options,
        Some(dataSchema),
        FileStatusCache.getOrCreate(session))
        .listFiles(Nil, Nil)
        .flatMap(_.files)
        .filter(_.isFile)

      SegmentDirectory(segID, files)
    }).filter(_.files.nonEmpty)
  }

  override lazy val partitionSchema: StructType = {
    // we did not use the partitionBy mechanism of spark
    new StructType()
  }

  lazy val timePartitionSchema: StructType = {
    val desc: PartitionDesc = dataflow.getModel.getPartitionDesc
    StructType(
      if (desc != null) {
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

  lazy val shardBySchema: StructType = {
    val shardByCols = layout.getShardByColumns
    StructType(
      if (shardByCols.isEmpty) {
        Seq.empty
      } else {
        require(shardByCols.size() == 1, "Now we only support one shard by col.")
        val id = shardByCols.asScala.head
        dataSchema.filter(_.name == id.toString)
      })
  }

  // timePartitionColumn is the mechanism of kylin.
  private var timePartitionColumn: Attribute = _

  private var shardByColumn: Attribute = _

  private var isResolved: Boolean = false

  def resolve(relation: LogicalRelation, resolver: Resolver): Unit = {
    val timePartitionAttr = relation.resolve(timePartitionSchema, resolver)
    if (timePartitionAttr.nonEmpty) {
      timePartitionColumn = timePartitionAttr.head
    }

    val shardByAttr = relation.resolve(shardBySchema, resolver)
    if (shardByAttr.nonEmpty) {
      shardByColumn = shardByAttr.head
    }
    isResolved = true
  }

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    require(isResolved)
    val timePartitionFilters = getSpecFilter(dataFilters, timePartitionColumn)
    logInfo(s"Applying time partition filters: ${timePartitionFilters.mkString(",")}")

    var selected = afterPruning("segment", timePartitionFilters, segmentDirs) {
      pruneSegments
    }

    selected = afterPruning("shard", dataFilters, selected) {
      pruneShards
    }

    if (selected.isEmpty) {
      Seq.empty
    } else {
      Seq(PartitionDirectory(InternalRow.empty, selected.flatMap(_.files)))
    }

  }

  private def afterPruning(pruningType: String, specFilters: Seq[Expression], inputs: Seq[SegmentDirectory])
                          (pruningFunc: (Seq[Expression], Seq[SegmentDirectory]) => Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    val filteredPartitions = if (specFilters.isEmpty) {
      inputs
    } else {
      var selected = inputs
      try {
        val startTime = System.nanoTime
        selected = pruningFunc(specFilters, inputs)
        val endTime = System.nanoTime()
        logInfo(s"$pruningType pruning in ${(endTime - startTime).toDouble / 1000000} ms")
      } catch {
        case th: Throwable =>
          logError(s"Error occurs when $pruningType, scan all ${pruningType}s.", th)
      }
      selected
    }

    logInfo(s"Selected files after $pruningType pruning:" + filteredPartitions.flatMap(_.files).map(_.getPath.toString).mkString(";"))

    filteredPartitions
  }

  private def getSpecFilter(dataFilters: Seq[Expression], col: Attribute): Seq[Expression] = {
    dataFilters.filter(_.references.subsetOf(AttributeSet(col)))
  }

  private def pruneSegments(filters: Seq[Expression],
                            segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {

    // reduce filters to supported only
    val reducedFilter = filters.flatMap(DataSourceStrategy.translateFilter).reduceLeft(And)

    val filteredStatuses = segDirs.filter {
      e => {
        val tsRange = dataflow.getSegment(e.segmentID).getTSRange
        SegFilters(tsRange.getStart, tsRange.getEnd).foldFilter(reducedFilter) match {
          case Trivial(true) => true
          case Trivial(false) => false
        }
      }
    }
    filteredStatuses
  }

  private def pruneShards(filters: Seq[Expression],
                          segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    val filteredStatuses = if (layout.getShardByColumns.isEmpty) {
      segDirs
    } else {
      val normalizedFiltersAndExpr = filters.reduce(expressions.And)

      segDirs.map { case SegmentDirectory(segID, files) =>
        val partitionNumber = dataflow.getSegment(segID).getLayout(layout.getId).getPartitionNum
        require(partitionNumber > 0, "Shards num with shard by col should greater than 0.")

        val bitSet = getExpressionBuckets(normalizedFiltersAndExpr, shardByColumn.name, partitionNumber)

        val selected = files.filter(f => {
          // path like: part-00001-91f13932-3d5e-4f85-9a56-d1e2b47d0ccb-c000.snappy.parquet
          // we need to get 00001.
          val split = f.getPath.getName.split("-", 3)(1).toInt
          bitSet.get(split)
        })
        SegmentDirectory(segID, selected)
      }
    }
    filteredStatuses
  }

  override lazy val inputFiles: Array[String] = segmentDirs.flatMap { e =>
    e.files.map(_.getPath.toString)
  }.toArray

  override lazy val sizeInBytes: Long = segmentDirs.flatMap { e =>
    e.files.map(_.getLen)
  }.sum

  override def refresh(): Unit = {}

  private def getExpressionBuckets(expr: Expression,
                                   bucketColumnName: String,
                                   numBuckets: Int): BitSet = {

    def getBucketNumber(attr: Attribute, v: Any): Int = {
      BucketingUtils.getBucketIdFromValue(attr, numBuckets, v)
    }

    def getBucketSetFromIterable(attr: Attribute, iter: Iterable[Any]): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      iter
        .map(v => getBucketNumber(attr, v))
        .foreach(bucketNum => matchedBuckets.set(bucketNum))
      matchedBuckets
    }

    def getBucketSetFromValue(attr: Attribute, v: Any): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      matchedBuckets.set(getBucketNumber(attr, v))
      matchedBuckets
    }

    expr match {
      case expressions.Equality(a: Attribute, Literal(v, _)) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, v)
      case expressions.In(a: Attribute, list)
        if list.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
        getBucketSetFromIterable(a, list.map(e => e.eval(EmptyRow)))
      case expressions.InSet(a: Attribute, hset)
        if hset.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
        getBucketSetFromIterable(a, hset.map(e => expressions.Literal(e).eval(EmptyRow)))
      case expressions.IsNull(a: Attribute) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, null)
      case expressions.And(left, right) =>
        getExpressionBuckets(left, bucketColumnName, numBuckets) &
          getExpressionBuckets(right, bucketColumnName, numBuckets)
      case expressions.Or(left, right) =>
        getExpressionBuckets(left, bucketColumnName, numBuckets) |
          getExpressionBuckets(right, bucketColumnName, numBuckets)
      case _ =>
        val matchedBuckets = new BitSet(numBuckets)
        matchedBuckets.setUntil(numBuckets)
        matchedBuckets
    }
  }
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