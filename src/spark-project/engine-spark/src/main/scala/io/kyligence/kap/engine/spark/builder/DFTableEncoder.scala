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
package io.kyligence.kap.engine.spark.builder

import java.util

import io.kyligence.kap.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil._
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.dict.NGlobalDictionaryV2
import org.apache.spark.internal.Logging
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable._

object DFTableEncoder extends Logging {

  def encodeTable(ds: Dataset[Row], seg: NDataSegment, cols: util.Set[TblColRef]): Dataset[Row] = {
    val structType = ds.schema
    var partitionedDs = ds

    ds.sparkSession.sparkContext.setJobDescription("Encode count source data.")
    val sourceCnt = ds.count()
    val bucketThreshold = seg.getConfig.getGlobalDictV2ThresholdBucketSize
    val minBucketSize: Long = sourceCnt / bucketThreshold

    var encodingCols = scala.collection.mutable.Set.empty[TblColRef]

    if (seg.getIndexPlan.isSkipEncodeIntegerFamilyEnabled) {
      encodingCols = cols.asScala.filterNot(_.getType.isIntegerFamily)

      val noEncodeCols = cols.asScala.filter(_.getType.isIntegerFamily).map {
        ref =>
          val encodeColRef = convertFromDot(ref.getIdentity)
          val aliasName = encodeColRef.concat(ENCODE_SUFFIX)
          col(encodeColRef).cast(LongType).as(aliasName)
      }.toSeq
      partitionedDs = partitionedDs.select(partitionedDs.schema.map(ty => col(ty.name)) ++ noEncodeCols : _*)
    } else {
      encodingCols = cols.asScala
    }

    val encodingArgs = encodingCols.map {
      ref =>
        val globalDict = new NGlobalDictionaryV2(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
        val bucketSize = globalDict.getBucketSizeOrDefault(seg.getConfig.getGlobalDictV2MinHashPartitions)
        val enlargedBucketSize = (((minBucketSize / bucketSize) + 1) * bucketSize).toInt

        val encodeColRef = convertFromDot(ref.getIdentity)
        val columnIndex = structType.fieldIndex(encodeColRef)

        val dictParams = Array(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
          .mkString(SEPARATOR)
        val aliasName = structType.apply(columnIndex).name.concat(ENCODE_SUFFIX)
        val encodeCol = dict_encode(col(encodeColRef).cast(StringType), lit(dictParams), lit(bucketSize).cast(StringType)).as(aliasName)
        val columns = encodeCol
        (enlargedBucketSize, col(encodeColRef).cast(StringType), columns, aliasName,
          bucketSize == 1)
      }

    if (KylinBuildEnv.get() != null && KylinBuildEnv.get().encodingDataSkew) {
      encodingArgs.foreach {
        case (enlargedBucketSize, repartitionColumn, projects, aliasName, false) =>
          val ds = partitionedDs.filter(repartitionColumn.isNull).select(partitionedDs.schema.map(sc => col(sc.name))
            ++ Seq(lit(null).as(aliasName)): _*)
          val encoding = partitionedDs.filter(repartitionColumn.isNotNull)
            .repartition(enlargedBucketSize, repartitionColumn)
            .select(partitionedDs.schema.map(ty => col(ty.name)) ++ Seq(projects): _*)
          partitionedDs = ds.union(encoding)
        case (_, _, projects, _, true) =>
          partitionedDs = partitionedDs
            .select(partitionedDs.schema.map(ty => col(ty.name)) ++ Seq(projects): _*)
      }
    } else {
      encodingArgs.foreach {
        case (enlargedBucketSize, repartitionColumn, projects, _, false) =>
          partitionedDs = partitionedDs
            .repartition(enlargedBucketSize, repartitionColumn)
            .select(partitionedDs.schema.map(ty => col(ty.name)) ++ Seq(projects): _*)
        case (_, _, projects, _, true) =>
          partitionedDs = partitionedDs
            .select(partitionedDs.schema.map(ty => col(ty.name)) ++ Seq(projects): _*)
      }
    }
    ds.sparkSession.sparkContext.setJobDescription(null)
    partitionedDs
  }
}