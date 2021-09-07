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

import io.kyligence.kap.engine.spark.builder.SegmentFlatTable.{Statistics, changeSchemeToColumnId}
import io.kyligence.kap.engine.spark.model.MLPFlatTableDesc
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.Objects
import java.{lang, util}
import scala.collection.JavaConverters._

class MLPFlatTable(private val sparkSession: SparkSession, //
                   private val tableDesc: MLPFlatTableDesc) extends SegmentFlatTable(sparkSession, tableDesc) {

  override protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Multi level partition.
    val descMLP = dataModel.getMultiPartitionDesc
    require(Objects.nonNull(descMLP))
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    val condition = descMLP.getPartitionConditionBuilder
      .buildMultiPartitionCondition(descDRP, descMLP, //
        new util.LinkedList[lang.Long](tableDesc.getPartitionIDs), null, segmentRange)
    if (StringUtils.isBlank(condition)) {
      logInfo(s"No available PARTITION-CONDITION segment $segmentId")
      return originDS
    }
    logInfo(s"Apply PARTITION-CONDITION $condition segment $segmentId")
    originDS.where(condition)
  }

  def getPartitionDS(partitionId: java.lang.Long, tableDS: Dataset[Row]): Dataset[Row] = {
    val columnIds = tableDesc.getColumnIds.asScala
    val columnName2Id = tableDesc.getColumns //
      .asScala //
      .map(column => column.getIdentity) //
      .zip(columnIds) //
    val column2IdMap = columnName2Id.toMap

    val partitionColumnIds = dataModel.getMultiPartitionDesc.getColumnRefs.asScala //
      .map(_.getIdentity).map(x => column2IdMap.apply(x))
    val values = dataModel.getMultiPartitionDesc.getPartitionInfo(partitionId).getValues.toSeq

    val converted = partitionColumnIds.zip(values).map { case (k, v) =>
      s"`$k` = '$v'"
    }.mkString(" and ")

    logInfo(s"Single PARTITION-CONDITION: $converted")
    tableDS.where(converted)
  }

  def getPartitionDS(partitionId: java.lang.Long): Dataset[Row] = {
    getPartitionDS(partitionId, FLAT_TABLE)
  }

  def getFactTablePartitionDS(partitionId: java.lang.Long): Dataset[Row] = {
    getPartitionDS(partitionId, changeSchemeToColumnId(fastFactTableWithFilterConditionTableDS, tableDesc))
  }

  def gatherPartitionStatistics(partitionId: Long, tableDS: Dataset[Row]): Statistics = {
    logInfo(s"Segment $segmentId gather statistics PARTITION-FLAT-TABLE $partitionId")
    sparkSession.sparkContext.setJobDescription(s"Segment $segmentId gather statistics PARTITION-FLAT-TABLE $partitionId")
    val statistics = gatherStatistics(tableDS)
    sparkSession.sparkContext.setJobDescription(null)
    statistics
  }

  def gatherPartitionColumnBytes(partitionId: Long, tableDS: Dataset[Row]): Map[String, Long] = {
    logInfo(s"Segment $segmentId gather statistics PARTITION-FLAT-TABLE $partitionId")
    sparkSession.sparkContext.setJobDescription(s"Segment $segmentId gather statistics PARTITION-FLAT-TABLE $partitionId")
    val statistics = gatherColumnBytes(tableDS)
    sparkSession.sparkContext.setJobDescription(null)
    statistics
  }

}
