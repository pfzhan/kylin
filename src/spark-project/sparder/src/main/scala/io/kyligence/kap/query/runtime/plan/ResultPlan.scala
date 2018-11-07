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
package io.kyligence.kap.query.runtime.plan

import com.google.common.collect.Lists
import io.kyligence.kap.query.runtime.plan.ResultType.ResultType
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.kylin.common.exceptions.KylinTimeoutException
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, SparderEnv}

import scala.collection.JavaConverters._

// scalastyle:off
object ResultType extends Enumeration {
  type ResultType = Value
  val ASYNC, NORMAL, SCALA = Value
}

object ResultPlan extends Logging {

  /*
    def asyncResult(df: DataFrame, rowType: RelDataType): Enumerable[Array[Any]] = {
      val separator = SparderEnv.getSeparator
      val path = KapConfig.getInstanceFromEnv.getAsyncResultBaseDir(QueryContext.current.getProject) +
        "/" + QueryContext.current.getQueryId
      val indexDataType = rowType.getFieldList.asScala.toList.zipWithIndex.map {
        case (field, index) =>
          (index, field.getType.getSqlTypeName.getName)
      }.toMap
      SparkOperation.export(df, indexDataType, separator, path)
      Linq4j.asEnumerable(Array.empty[Array[Any]])
    }
   */

  def collectEnumerable(df: DataFrame,
                        rowType: RelDataType): Enumerable[Array[Any]] =
    withScope(df, rowType) {
      val rowsItr: Array[Array[Any]] = collectInternal(df, rowType)
      Linq4j.asEnumerable(rowsItr.array)
    }

  def collectScalarEnumerable(df: DataFrame,
                              rowType: RelDataType): Enumerable[Any] =
    withScope(df, rowType) {
      val rowsItr: Array[Array[Any]] = collectInternal(df, rowType)
      val x = rowsItr.toIterable.map(a => a.apply(0)).asJava
      Linq4j.asEnumerable(x)
    }

  private def collectInternal(df: DataFrame,
                              rowType: RelDataType): Array[Array[Any]] = {
    val resultTypes = rowType.getFieldList.asScala
    val jobGroup = Thread.currentThread().getName
    val sparkContext = SparderEnv.getSparkSession.sparkContext
    // touch, so that scan exec can be executed.
    df.queryExecution.sparkPlan

    val kapConfig = KapConfig.getInstanceFromEnv
    var pool = "heavy_tasks"

    // set priority
    sparkContext.setLocalProperty("spark.scheduler.pool", pool)

    sparkContext.setJobGroup(jobGroup,
                             QueryContext.current().getSql,
                             interruptOnCancel = true)
    try {
      val rows = df.collect()
      val dt = rows.map { row =>
        var rowIndex = 0
        row.toSeq.map { cell =>
          {
            var vale = cell
            val rType = resultTypes.apply(rowIndex).getType
            val value = SparderTypeUtil.convertStringToValue(vale,
                                                             rType,
                                                             toCalcite = true)
            rowIndex = rowIndex + 1
            value
          }
        }.toArray
      }
      dt
      //      }
    } catch {
      case e: InterruptedException =>
        QueryContext.current().setTimeout(true)
        sparkContext.cancelJobGroup(jobGroup)
        logInfo(
          s"Query timeouts after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s",
          e)
        throw new KylinTimeoutException(
          s"Query timeout after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s");
    }
  }

  /**
    * use to check acl  or other
    *
    * @param df      finally df
    * @param rowType result rowType
    * @param body    resultFunc
    * @tparam U
    * @return
    */
  def withScope[U](df: DataFrame, rowType: RelDataType)(body: => U): U = {
    body
  }

  def getResult(df: DataFrame, rowType: RelDataType, resultType: ResultType)
    : Either[Enumerable[Array[Any]], Enumerable[Any]] = {
    SparderEnv.fuseResistor.backoffIfNecessary()
    SparderEnv.setDF(df)
    val result: Either[Enumerable[Array[Any]], Enumerable[Any]] =
      resultType match {
        case ResultType.NORMAL =>
          if ("true".equals(System.getProperty("skipCompute"))) {
            Left(Linq4j.asEnumerable(Array.empty[Array[Any]]))
          } else {
            Left(ResultPlan.collectEnumerable(df, rowType))
          }
        case ResultType.SCALA =>
          if ("true".equals(System.getProperty("skipCompute"))) {
            Right(Linq4j.asEnumerable(Lists.newArrayList[Any]()))
          } else {
            Right(ResultPlan.collectScalarEnumerable(df, rowType))

          }
        //        case ResultType.ASYNC =>
        //          Left(ResultPlan.asyncResult(df, rowType))
      }
    SparderEnv.cleanQueryInfo()
    result
  }
}
