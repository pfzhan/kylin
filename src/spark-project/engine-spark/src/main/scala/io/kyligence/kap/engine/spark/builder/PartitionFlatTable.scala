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

import java.util.Objects
import java.{lang, util}

import io.kyligence.kap.engine.spark.builder.SegmentFlatTable.{Statistics, changeSchemeToColumnId}
import io.kyligence.kap.metadata.cube.model.PartitionFlatTableDesc
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

class PartitionFlatTable(private val sparkSession: SparkSession, //
                         private val tableDesc: PartitionFlatTableDesc) extends SegmentFlatTable(sparkSession, tableDesc) {

  override protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Multi level partition.
    val descMLP = dataModel.getMultiPartitionDesc
    require(Objects.nonNull(descMLP))
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    val condition = descMLP.getPartitionConditionBuilder
      .buildMultiPartitionCondition(descDRP, descMLP, //
        new util.LinkedList[lang.Long](tableDesc.getPartitions), null, segmentRange)
    if (StringUtils.isBlank(condition)) {
      logInfo(s"Segment $segmentId no available partition condition.")
      return originDS
    }
    logInfo(s"Segment $segmentId apply partition condition $condition.")
    originDS.where(condition)
  }

  def getPartitionDS(partition: Long): Dataset[Row] = {
    getPartitionDS(partition, FLAT_TABLE)
  }

  def getFactTablePartitionDS(partition: Long): Dataset[Row] = {
    getPartitionDS(partition, changeSchemeToColumnId(fastFactTableWithFilterConditionTableDS, tableDesc))
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

  def gatherPartitionColumnBytes(partition: Long, tableDS: Dataset[Row]): Map[String, Long] = {
    val desc = s"Segment $segmentId collect partition flat table statistics $partition"
    logInfo(desc)
    sparkSession.sparkContext.setJobDescription(desc)
    val statistics = gatherColumnBytes(tableDS)
    sparkSession.sparkContext.setJobDescription(null)
    statistics
  }

  private def getPartitionDS(partition: Long, tableDS: Dataset[Row]): Dataset[Row] = {
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
    tableDS.where(converted)
  }

}
