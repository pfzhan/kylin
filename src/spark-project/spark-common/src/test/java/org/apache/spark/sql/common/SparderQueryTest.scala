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
package org.apache.spark.sql.common

import java.io.File
import java.sql.Types
import java.util.{List, TimeZone}

import io.kyligence.kap.metadata.query.StructField
import org.apache.commons.io.FileUtils
import org.apache.kylin.common.{KapConfig, QueryContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparderQueryTest extends Logging {

  def same(sparkDF: DataFrame,
                     kylinAnswer: DataFrame,
                     checkOrder: Boolean = false): Boolean = {
    checkAnswerBySeq(castDataType(sparkDF, kylinAnswer), kylinAnswer.collect(), checkOrder) match {
      case Some(errorMessage) =>
        logInfo(errorMessage)
        false
      case None => true
    }
  }

  def checkAnswer(sparkDF: DataFrame,
                  kylinAnswer: DataFrame,
                  checkOrder: Boolean = false): String = {
    checkAnswerBySeq(castDataType(sparkDF, kylinAnswer), kylinAnswer.collect(), checkOrder) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }

  /**
    * Runs the plan and makes sure the answer matches the expected result.
    * If there was exception during the execution or the contents of the DataFrame does not
    * match the expected result, an error message will be returned. Otherwise, a [[None]] will
    * be returned.
    *
    * @param sparkDF     the [[org.apache.spark.sql.DataFrame]] to be executed
    * @param kylinAnswer the expected result in a [[Seq]] of [[org.apache.spark.sql.Row]]s.
    * @param checkToRDD  whether to verify deserialization to an RDD. This runs the query twice.
    */
  def checkAnswerBySeq(sparkDF: DataFrame,
                       kylinAnswer: Seq[Row],
                       checkOrder: Boolean = false,
                       checkToRDD: Boolean = true): Option[String] = {
    if (checkToRDD) {
      sparkDF.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
    }

    val sparkAnswer = try sparkDF.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${sparkDF.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(sparkAnswer, kylinAnswer, checkOrder).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
        |${sparkDF.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // compare string .
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP)
      case db: Double if db.isNaN || db.isInfinite => None
      case db: Double => BigDecimal.apply(db).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString.toDouble
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  def sameRows(sparkAnswer: Seq[Row],
               kylinAnswer: Seq[Row],
               isSorted: Boolean = false): Option[String] = {
    if (prepareAnswer(sparkAnswer, isSorted) != prepareAnswer(kylinAnswer,
      isSorted)) {
      val errorMessage =
        s"""
           |== Results ==
           |${
          sideBySide(
            s"== Kylin Answer - ${kylinAnswer.size} ==" +:
              prepareAnswer(kylinAnswer, isSorted).map(_.toString()),
            s"== Spark Answer - ${sparkAnswer.size} ==" +:
              prepareAnswer(sparkAnswer, isSorted).map(_.toString())
          ).mkString("\n")
        }
        """.stripMargin
      return Some(errorMessage)
    }
    None
  }


  /**
    * Runs the plan and makes sure the answer is within absTol of the expected result.
    *
    * @param actualAnswer   the actual result in a [[Row]].
    * @param expectedAnswer the expected result in a[[Row]].
    * @param absTol         the absolute tolerance between actual and expected answers.
    */
  protected def checkAggregatesWithTol(actualAnswer: Row,
                                       expectedAnswer: Row,
                                       absTol: Double) = {
    require(actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(
          math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAsyncResultData(dataFrame: DataFrame,
                           sparkSession: SparkSession): Unit = {
    val path = KapConfig.getInstanceFromEnv.getAsyncResultBaseDir() + "/" +
      QueryContext.current.getQueryId
    val rows = sparkSession.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .load(path)
    val maybeString = checkAnswer(dataFrame, rows)

  }

  private def isCharType(dataType: Integer): Boolean = {
    if (dataType == Types.CHAR || dataType == Types.VARCHAR) {
      return true
    }
    false
  }

  private def isIntType(structField: StructField): Boolean = {
    val intList = scala.collection.immutable.List(Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT)
    val dataType = structField.getDataType();
    if (intList.contains(dataType)) {
      return true
    }
    false
  }

  private def isSameDataType(cubeStructField: StructField, sparkStructField: StructField): Boolean = {
    if (cubeStructField.getDataType() == sparkStructField.getDataType()) {
      return true
    }
    // calcite dataTypeName = "ANY"
    if (cubeStructField.getDataType() == 2000) {
      return true
    }
    if (isCharType(cubeStructField.getDataType()) && isCharType(sparkStructField.getDataType())) {
      return true
    }
    if (isIntType(cubeStructField) && isIntType(sparkStructField)) {
      return true
    }
    false
  }

  def compareColumnTypeWithCalcite(cubeSchema: List[StructField], sparkSchema: StructType): Unit = {
    val cubeSize = cubeSchema.size
    val sparkSize = sparkSchema.size
    assert(cubeSize == sparkSize, s"$cubeSize did not equal $sparkSize")
    for (i <- 0 to cubeSize - 1) {
      val cubeStructField = cubeSchema.get(i)
      val sparkStructField = SparderTypeUtil.convertSparkFieldToJavaField(sparkSchema.apply(i))
      assert(isSameDataType(cubeStructField, sparkStructField),
        s"${cubeStructField.getDataTypeName()} did not equal ${sparkSchema.apply(i).dataType.toString()}")
    }
  }

  def castDataType(sparkResult: DataFrame, cubeResult: DataFrame): DataFrame = {
    val newNames = sparkResult.schema.names
      .zipWithIndex
      .map(name => name._1.replaceAll("\\.", "_") + "_" + name._2).toSeq
    val newDf = sparkResult.toDF(newNames: _*)
    val columns = newDf.schema.zip(cubeResult.schema).map {
      case (sparkField, kylinField) =>
        if (!sparkField.dataType.sameType(kylinField.dataType)) {
          col(sparkField.name).cast(kylinField.dataType)
        } else {
          col(sparkField.name)
        }
    }
    newDf.select(columns: _*)
  }
}
