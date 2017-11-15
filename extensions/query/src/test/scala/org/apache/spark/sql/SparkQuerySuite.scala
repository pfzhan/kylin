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

package org.apache.spark.sql

import java.io.ObjectInputStream
import java.nio.ByteBuffer

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest
import io.kyligence.kap.storage.parquet.cube.spark.rpc.sparder.KylinCast
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.gridtable.GTScanRequest
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.sparder._
import org.apache.spark.sql.execution.utils.HexUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.udf.{AggraFuncArgs, AggraFuncArgs2, SparderAggFun}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._
import scala.io.Source

class SparkQuerySuite extends FunSuite
  with BeforeAndAfterAll {
  val cuboid = 114696L
  val payloadPath: String = TestConstants.TEST_WORK_DIR + "payload"
  val gtinfoPath: String = TestConstants.TEST_WORK_DIR + "gtinfo"
  val KYLIN_SCAN_GTINFO_BYTES = "io.kylin.storage.parquet.scan.gtinfo"
  val COLUMN_PREFIX = "col_"
  val CUBOID_ID = "cuboid_id"
  val FILTER_PUSH_DOWN = "filter_push_down"
  KylinConfig.setKylinConfigInEnvIfMissing(sparkJobRequest.getPayload.getKylinProperties)
  val fileSeq: Seq[String] = getCubeFileName(cuboid).map(sandboxPath => TestConstants.TEST_WORK_DIR + "cube/" + sandboxPath.getName)
  var spark: SparkSession = _
  var format: SparderFileFormat = _
  var hadoopConf: Configuration = _
  var sparkJobRequest: SparkJobRequest = {
    val str: String = Source.fromFile(payloadPath).mkString
    val payload: SparkJobProtos.SparkJobRequestPayload = SparkJobProtos.SparkJobRequestPayload.newBuilder().mergeFrom(HexUtils.toBytes(str)).build()
    SparkJobRequest.newBuilder.setPayload(payload).build
  }
  KylinConfig.setKylinConfigInEnvIfMissing(sparkJobRequest.getPayload.getKylinProperties)
  var scanRequest: GTScanRequest = TestGtscanRequest.serializer.deserialize(ByteBuffer.wrap(sparkJobRequest.getPayload.getGtScanRequest.toByteArray))

  var gtInfoStr: String = Source.fromFile(gtinfoPath).mkString

  override def beforeAll() {
    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions", "1")
    spark = SparkSession.builder
      .master("local[1]")
      .appName("test-sql-context")
      .config(conf)
      .getOrCreate()
    format = new SparderFileFormat()
    hadoopConf = spark.sparkContext.hadoopConfiguration
    val cleanMap = cleanRequest(scanRequest, gtInfoStr)
    val intToString = register(cleanMap, spark, cuboid.toString)
    val scanRequestStr: String = new String(scanRequest.toByteArray, "ISO-8859-1")
    spark.udf.register("kylin_castb", new KylinCast(gtInfoStr), DecimalType(19, 4))
    spark.udf.register("kylin_casta", new KylinCast(gtInfoStr), LongType)
  }


  def getCubeFileName(cubeid: Long): Seq[Path] = {
    val cubeInfoPath = new Path(TestConstants.TEST_WORK_DIR + "cube/CUBE_INFO")
    var map: java.util.Map[Long, java.util.Set[String]] = null
    val inputStream = cubeInfoPath.getFileSystem(new Configuration()).open(cubeInfoPath)
    try {
      val obj: ObjectInputStream = new ObjectInputStream(inputStream)
      try {
        map = obj.readObject.asInstanceOf[java.util.Map[Long, java.util.Set[String]]]
      } finally if (inputStream != null) inputStream.close()
    }
    val paths = map.get(cubeid)
    paths.asScala.map(new Path(_)).toSeq
  }


  def builSchema(): StructType = {
    StructType(scanRequest.getInfo.getAllColumns.asScala.map(i => StructField(s"col_$i", BinaryType, false)).toSeq)

  }


  def cleanRequest(gTScanRequest: GTScanRequest, gTinfoStr: String): Map[Int, AggraFuncArgs] = {

    gTScanRequest.getAggrMetrics.iterator().asScala
      .zip(gTScanRequest.getAggrMetricsFuncs.iterator)
      .map {
        case (colId, funName) =>
          (colId.toInt, AggraFuncArgs(funName, gTScanRequest.getInfo.getColumnType(colId), gTinfoStr))
      }.toMap
  }


  def register(aggrMap: Map[Int, AggraFuncArgs], sparkSession: SparkSession, tableName: String): Map[Int, String] = {
    aggrMap.map {
      case (colId, aggr) =>
        val name = aggr.dataTp.toString.replace("(", "_").replace(")", "_").replace(",", "_") + aggr.funcName

        spark.udf.register(name, new SparderAggFun(AggraFuncArgs2(aggr.funcName, aggr.dataTp)))
        (colId, name)
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
}
