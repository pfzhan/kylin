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

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Cast, Expression, InterpretedPredicate, Literal}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class LayoutFileIndex(
  sparkSession: SparkSession,
  catalogTable: CatalogTable,
  override val sizeInBytes: Long,
  segments: Seq[NDataSegment]) extends CatalogFileIndex(sparkSession, catalogTable, sizeInBytes) {

  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

  lazy val paths: Seq[Path] = {
    segments
      .map(seg => new Path(NSparkCubingUtil.getStoragePath(seg, catalogTable.identifier.table.toLong)))
  }

  override def rootPaths: Seq[Path] = paths

  override def filterPartitions(partitionFilters: Seq[Expression]): InMemoryFileIndex = {

    if (catalogTable.partitionColumnNames.nonEmpty  ) {
      val partitionSchema = catalogTable.partitionSchema
      val partitionColumnNames = catalogTable.partitionColumnNames.toSet
      val nonPartitionPruningPredicates = partitionFilters.filterNot {
        _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
      }
      if (nonPartitionPruningPredicates.nonEmpty) {
        throw new AnalysisException("Expected only partition pruning predicates:" + nonPartitionPruningPredicates)
      }
       var partitionPaths = segments
         .flatMap { seg =>
           val layout = seg.getLayout(catalogTable.identifier.table.toLong)
           val data = layout.getPartitionValues
           val baseDir = NSparkCubingUtil.getStoragePath(seg, layout.getLayoutId)
           data.asScala.map(dt => (dt, baseDir + "/" + dt))
         }.map { tp =>
        val spec = PartitioningUtils.parsePathFragment(tp._1)
        val row = InternalRow.fromSeq(partitionSchema.map { field =>
          val partValue = if (spec(field.name) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
            null
          } else {
            spec(field.name)
          }
          Cast(Literal(partValue), field.dataType, Option(SQLConf.get.sessionLocalTimeZone)).eval()
        })
        PartitionPath(row, new Path(tp._2))
      }
      if (partitionFilters.nonEmpty) {
        val boundPredicate =
          InterpretedPredicate.create(partitionFilters.reduce(And).transform {
            case att: AttributeReference =>
              val index = partitionSchema.indexWhere(_.name == att.name)
              BoundReference(index, partitionSchema(index).dataType, nullable = true)
          })
        partitionPaths = partitionPaths.filter(partitionPath => boundPredicate.eval(partitionPath.values))
      }
      new PrunedInMemoryFileIndex(sparkSession, fileStatusCache, PartitionSpec(partitionSchema, partitionPaths))
    } else {
      new InMemoryFileIndex(
        sparkSession, rootPaths, Map.empty[String, String], userSpecifiedSchema = Option(table.schema))
    }
  }

  override def partitionSchema: StructType = catalogTable.partitionSchema
}
private class PrunedInMemoryFileIndex(
  sparkSession: SparkSession,
  fileStatusCache: FileStatusCache,
  override val partitionSpec: PartitionSpec)
  extends InMemoryFileIndex(
    sparkSession,
    partitionSpec.partitions.map(_.path),
    Map.empty,
    Some(partitionSpec.partitionColumns),
    fileStatusCache)