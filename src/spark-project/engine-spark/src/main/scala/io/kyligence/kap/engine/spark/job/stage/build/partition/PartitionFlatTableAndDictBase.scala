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

package io.kyligence.kap.engine.spark.job.stage.build.partition

import io.kyligence.kap.engine.spark.builder.{DictionaryBuilderHelper, PartitionDictionaryBuilderHelper}
import io.kyligence.kap.engine.spark.job.stage.BuildParam
import io.kyligence.kap.engine.spark.job.stage.build.FlatTableAndDictBase
import io.kyligence.kap.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import io.kyligence.kap.engine.spark.job.{PartitionExec, SegmentJob}
import io.kyligence.kap.engine.spark.model.PartitionFlatTableDesc
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.{Dataset, Row}

import java.util.Objects
import java.{lang, util}
import scala.collection.JavaConverters._

abstract class PartitionFlatTableAndDictBase(private val jobContext: SegmentJob,
                                             private val dataSegment: NDataSegment,
                                             private val buildParam: BuildParam)
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) with PartitionExec {

  protected final val newBuckets = //
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq


  override protected lazy val spanningTree = buildParam.getPartitionSpanningTree
  override protected lazy val tableDesc = buildParam.getTableDesc

  override protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Multi level partition.
    val descMLP = dataModel.getMultiPartitionDesc
    require(Objects.nonNull(descMLP))
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    val condition = descMLP.getPartitionConditionBuilder
      .buildMultiPartitionCondition(descDRP, descMLP, //
        new util.LinkedList[lang.Long](tableDesc.asInstanceOf[PartitionFlatTableDesc].getPartitions), null, segmentRange)
    if (StringUtils.isBlank(condition)) {
      logInfo(s"Segment $segmentId no available partition condition.")
      return originDS
    }
    logInfo(s"Segment $segmentId apply partition condition $condition.")
    originDS.where(condition)
  }

  def gatherPartitionStatistics(partition: Long, tableDS: Dataset[Row]): Statistics = {
    val desc = s"Segment $segmentId collect partition flat table statistics $partition"
    logInfo(desc)
    sparkSession.sparkContext.setJobDescription(desc)
    val statistics = gatherStatistics(tableDS)
    sparkSession.sparkContext.setJobDescription(null)
    logInfo(s"$desc $statistics")
    statistics
  }

  def getPartitionDS(partition: Long): Dataset[Row] = {
    val columnIds = tableDesc.getColumnIds.asScala
    val columnName2Id = tableDesc.getColumns //
      .asScala //
      .map(column => column.getIdentity) //
      .zip(columnIds) //
    val column2IdMap = columnName2Id.toMap

    val partitionColumnIds = dataModel.getMultiPartitionDesc.getColumnRefs.asScala //
      .map(_.getIdentity).map(x => column2IdMap.apply(x))
    val values = dataModel.getMultiPartitionDesc.getPartitionInfo(partition).getValues.toSeq

    val converted = partitionColumnIds.zip(values).map { case (k, v) =>
      s"`$k` = '$v'"
    }.mkString(" and ")

    logInfo(s"Segment $segmentId single partition condition: $converted")
    FLAT_TABLE.where(converted)
  }

  override def prepareForDict(): (Set[TblColRef], Set[TblColRef], Set[TblColRef], Set[TblColRef]) = {
    val dictCols = PartitionDictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(dataSegment, spanningTree.getIndices).asScala.toSet
    val encodeCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(dataSegment, spanningTree.getIndices).asScala.toSet
    val dictColsWithoutCc = dictCols.filter(!_.getColumnDesc.isComputedColumn)
    val encodeColsWithoutCc = encodeCols.filter(!_.getColumnDesc.isComputedColumn)
    (dictCols, encodeCols, dictColsWithoutCc, encodeColsWithoutCc)
  }
}
