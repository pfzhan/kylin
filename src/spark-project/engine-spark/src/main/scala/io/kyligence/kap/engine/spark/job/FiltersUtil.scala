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

package io.kyligence.kap.engine.spark.job

import io.kyligence.kap.engine.spark.builder.CreateFlatTable
import io.kyligence.kap.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.metadata.model.{JoinTableDesc, PartitionDesc, TblColRef}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.Set

object FiltersUtil extends Logging {

  private val allEqualsColSets: Set[String] = Set.empty


  private var flatTable: SegmentFlatTableDesc = _

  def initFilters(flatTable: SegmentFlatTableDesc,
                  lookupTableDatasetMap: mutable.Map[JoinTableDesc, Dataset[Row]]): Unit = {
    try {
      this.flatTable = flatTable
      if (flatTable.getDataModel.getPartitionDesc == null) {
        return
      }
      val parColumn = flatTable.getDataModel.getPartitionDesc.getPartitionDateColumnRef.toString
      logInfo(s"partition col is ${parColumn}")
      val allEqualColPairs = lookupTableDatasetMap.map(_._1.getJoin).
        flatMap(join => join.getForeignKeyColumns.map(_.toString).zip(join.getPrimaryKeyColumns.map(_.toString)))
      logInfo(s"allEqualColPairs is ${allEqualColPairs} ")
      allEqualsColSets += parColumn
      var singleRoundEqualColSets = allEqualColPairs.filter(_._1 == parColumn).map(_._2).toSet

      logInfo(s"first round equal col sets is ${singleRoundEqualColSets}")

      var subSet = singleRoundEqualColSets -- allEqualsColSets

      while (subSet.nonEmpty) {
        subSet = singleRoundEqualColSets -- allEqualsColSets
        logInfo(s"this round substract equal col set is ${subSet}")
        allEqualsColSets ++= subSet
        singleRoundEqualColSets = subSet.flatMap { col =>
          allEqualColPairs.filter(_._1 == col).map(_._2)
        }
        logInfo(s"this round equal col sets is ${singleRoundEqualColSets}")
      }
      logInfo(s"the allEqualsColSets is ${allEqualsColSets}")
    } catch {
      case e: Exception =>
        log.warn("init filters failed: Exception: ", e)
    }

  }

  def getAllEqualColSets(): Set[String] = {
    allEqualsColSets
  }

  def inferFilters(pks: Array[TblColRef],
                   ds: Dataset[Row]): Dataset[Row] = {
    // just consider one join key condition
    pks.filter(pk => allEqualsColSets.contains(pk.toString)).headOption match {
      case Some(col) =>
        var afterFilter = ds
        val model = flatTable.getDataModel
        val partDesc = PartitionDesc.getCopyOf(model.getPartitionDesc)
        partDesc.setPartitionDateColumnRef(col)
        if (partDesc != null && partDesc.getPartitionDateColumn != null) {
          val segRange = flatTable.getSegmentRange
          if (segRange != null && !segRange.isInfinite) {
            var afterConvertPartition = partDesc.getPartitionConditionBuilder
              .buildDateRangeCondition(partDesc, null, segRange)
            afterConvertPartition = CreateFlatTable.replaceDot(afterConvertPartition, model)
            logInfo(s"Partition filter $afterConvertPartition")
            afterFilter = afterFilter.where(afterConvertPartition)
          }
        }
        logInfo(s"after filter plan is ${
          afterFilter.queryExecution
        }")
        afterFilter
      case None =>
        ds
    }
  }
}
