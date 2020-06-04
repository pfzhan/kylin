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

import io.kyligence.kap.common.persistence.transaction.UnitOfWork
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil._
import io.kyligence.kap.metadata.cube.model.{NDataSegment, NDataflowManager, NDataflowUpdate}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.{TblColRef}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.util.{Failure, Success, Try}

object DFBuilderHelper extends Logging {

  val ENCODE_SUFFIX = "_KE_ENCODE"

  def filterCols(dsSeq: Seq[Dataset[Row]], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols -- dsSeq.flatMap(ds => filterCols(ds, needCheckCols))
  }

  def filterCols(ds: Dataset[Row], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols.filter(cc =>
      isValidExpr(convertFromDot(cc.getExpressionInSourceDB), ds))
  }

  def filterOutIntegerFamilyType(ds: Dataset[Row], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols.filterNot(_.getType.isIntegerFamily).filter(cc =>
      isValidExpr(convertFromDot(cc.getExpressionInSourceDB), ds))
  }

  def isValidExpr(colExpr: String, ds: Dataset[Row]): Boolean = {
    Try(ds.select(expr(colExpr))) match {
      case Success(_) =>
        true
      case Failure(_) =>
        false
    }
  }

  def chooseSuitableCols(ds: Dataset[Row], needCheckCols: Iterable[TblColRef]): Seq[Column] = {
    needCheckCols
      .filter(ref => isValidExpr(ref.getExpressionInSourceDB, ds))
      .map(ref => expr(convertFromDot(ref.getExpressionInSourceDB)).alias(convertFromDot(ref.getIdentity)))
      .toSeq
  }

  def checkPointSegment(readOnlySeg: NDataSegment, checkpointOps: NDataSegment => Unit): NDataSegment = {
    // read basic infos from the origin segment
    val segId = readOnlySeg.getId
    val dfId = readOnlySeg.getDataflow.getId
    val project = readOnlySeg.getProject

    // read the current config
    // this config is initialized at SparkApplication in which the HDFSMetaStore has been specified
    val config = KylinConfig.getInstanceFromEnv

    // copy the latest df & seg
    val dfCopy = NDataflowManager.getInstance(config, project).getDataflow(dfId).copy()
    val segCopy = dfCopy.getSegment(segId)
    val dfUpdate = new NDataflowUpdate(dfId)
    checkpointOps(segCopy)
    dfUpdate.setToUpdateSegs(segCopy)

    // define the updating operations
    class DataFlowUpdateOps extends UnitOfWork.Callback[NDataSegment] {
      override def process(): NDataSegment = {
        val updatedDf = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, project).updateDataflow(dfUpdate)
        updatedDf.getSegment(segId)
      }
    }

    // temporarily for ut
    // return the latest segment
    UnitOfWork.doInTransactionWithRetry(new DataFlowUpdateOps, project)
  }
}
