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

package org.apache.spark.sql.execution.datasources.sparder

import java.nio.ByteBuffer

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.gridtable.{GTInfo, GTScanRequest}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.utils.HexUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source

class SparderFunSuite extends FunSuite
  with BeforeAndAfterAll
  with Logging {
  val CUBE_FILE_DIR: String = TestConstants.TEST_WORK_DIR + "cube/"
  val path = new Path(TestConstants.TEST_WORK_DIR + "cube/peoLwWAlpT.parquettar")
  val cubeInfoPath = new Path(TestConstants.TEST_WORK_DIR + "cube/CUBE_INFO")
  val alias = "col"
  val cuboid: Long = 114696L
  val payloadPath: String = "payload"
  val gtinfoPath: String = "gtinfo"
  var sparkJobRequest: SparkJobRequest = {
    val str: String = Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream(payloadPath)).mkString
    val payload: SparkJobProtos.SparkJobRequestPayload = SparkJobProtos.SparkJobRequestPayload.newBuilder().mergeFrom(HexUtils.toBytes(str)).build()
    SparkJobRequest.newBuilder.setPayload(payload).build
  }
  KylinConfig.setKylinConfigInEnvIfMissing(sparkJobRequest.getPayload.getKylinProperties)
  val scanRequestArray: Array[Byte] = sparkJobRequest.getPayload.getGtScanRequest.toByteArray
  var scanRequest: GTScanRequest = TestGtscanRequest.serializer.deserialize(ByteBuffer.wrap(scanRequestArray))
  val gTInfo: GTInfo = scanRequest.getInfo
  var spark: SparkSession = _
  var format: SparderFileFormat = _
  var hadoopConf: Configuration = _

  var gtInfoStr: String = Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream(gtinfoPath)).mkString
  var sparkNeed = true
  var javaContext: JavaSparkContext = _

  override def beforeAll() {
    beforeInit()
    val conf = new SparkConf()
    if (sparkNeed) {
      val sparkConf = new SparkConf().setMaster("local[1]").setAppName("aha")

      javaContext = new JavaSparkContext(sparkConf)
      spark = SparkSession.builder
        .config("spark.executor.memory", "8g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.broadcast.blockSize", "512m")
        .config(javaContext.getConf)
        .getOrCreate()
      format = new SparderFileFormat()
      hadoopConf = spark.sparkContext.hadoopConfiguration
    }
    afterInit()

  }

  def beforeInit(): Unit = {

  }

  def afterInit(): Unit = {

  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    assertEmptyMissingInput(analyzedDF)

    QueryTest.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

}
