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

package io.kyligence.kap.it

import java.io.{DataInputStream, File, FileReader, PrintWriter}
import java.nio.charset.Charset
import java.nio.file.Files
import java.sql.SQLException
import java.util.TimeZone

import com.opencsv.CSVReaderHeaderAware
import io.kyligence.kap.common.util.Unsafe
import io.kyligence.kap.common.{CompareSupport, JobSupport, QuerySupport, SSSource}
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.metadata.cube.model.{IndexPlan, NDataflowManager, NIndexPlanManager}
import io.kyligence.kap.metadata.model.{NDataModel, NDataModelManager}
import io.kyligence.kap.query.engine.QueryExec
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.persistence.{JsonSerializer, RootPersistentEntity}
import org.apache.kylin.query.util.{QueryParams, QueryUtil}
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparderEnv}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.io.Source

// scalastyle:off
class TestODBCTDVTQuery
  extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with QuerySupport
    with CompareSupport
    with SSSource
    with AdaptiveSparkPlanHelper
    with LogEx {

  val dumpResult = false

  override val DEFAULT_PROJECT = "tdvt"

  case class FloderInfo(floder: String, filter: List[String] = List(), checkOrder: Boolean = false)

  val defaultTimeZone: TimeZone = TimeZone.getDefault

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  // opt memory
  conf.set("spark.shuffle.detectCorrupt", "false")

  override protected def getProject: String = DEFAULT_PROJECT

  override def beforeAll(): Unit = {
    Unsafe.setProperty("calcite.keep-in-clause", "true")
    Unsafe.setProperty("kylin.dictionary.null-encoding-opt-threshold", "1")
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))

    super.beforeAll()
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.pushdown.runner-class-name", "")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.pushdown-enabled", "false")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.snapshot.parallel-build-enabled", "true")

    addModels()

    SparderEnv.skipCompute()
    build()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SparderEnv.cleanCompute()
    TimeZone.setDefault(defaultTimeZone)
    Unsafe.clearProperty("calcite.keep-in-clause")
  }

  private def addModels(): Unit = {
    val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    modelMgr.createDataModelDesc(
      read(classOf[NDataModel], "model_desc/e31343d1-d921-4d61-83ec-5510b35a0409.json"), "ADMIN")
    modelMgr.createDataModelDesc(
      read(classOf[NDataModel], "model_desc/ee6d8257-684f-4dcb-b531-5dbf2b6ffd32.json"), "ADMIN")

    val indexPlanMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    val indexPlan1 = indexPlanMgr.createIndexPlan(
      read(classOf[IndexPlan], "index_plan/e31343d1-d921-4d61-83ec-5510b35a0409.json"))
    dfMgr.createDataflow(indexPlan1, "ADMIN")

    val indexPlan2 = indexPlanMgr.createIndexPlan(
      read(classOf[IndexPlan], "index_plan/ee6d8257-684f-4dcb-b531-5dbf2b6ffd32.json"))
    dfMgr.createDataflow(indexPlan2, "ADMIN")
  }

  private def read[T <: RootPersistentEntity](clz: Class[T], subPath: String): T = {
    val path = "src/test/resources/tdvt/" + subPath
    val contents = StringUtils.join(Files.readAllLines(new File(path).toPath, Charset.defaultCharset), "\n")
    val bais = IOUtils.toInputStream(contents, Charset.defaultCharset)
    new JsonSerializer[T](clz).deserialize(new DataInputStream(bais))
  }

  test("Test Tdvt") {
    logTime("test tdvt", debug = true) {
      val tdvtSqlsPath = "src/test/resources/tdvt/tdvt_sqls.csv"
      val tdvtResultPath = "src/test/resources/tdvt/expected_result"
      val tdvtDumpResultPath = "src/test/resources/tdvt/dump_expected_result"

      val dumpWriter = new PrintWriter(tdvtDumpResultPath)
      val expctedResultSource = Source.fromFile(tdvtResultPath)

      try {
        val expctedResult = expctedResultSource.getLines()
        val sqlReader = new CSVReaderHeaderAware(new FileReader(tdvtSqlsPath))
        var idx = 1;
        sqlReader.iterator().forEachRemaining(row => {
          val passed = row(0).toBoolean
          val sql = row(1)
          if (passed && !isEmpty(sql)) {
            logInfo(s"running tdvt sql$idx $sql")

            val queryParams = new QueryParams(
              KylinConfig.getInstanceFromEnv, sql, DEFAULT_PROJECT,
              0, 0, "TDVT", true)

            var df: Dataset[Row] = null
            try {
              new QueryExec(DEFAULT_PROJECT, KylinConfig.getInstanceFromEnv).executeQuery(QueryUtil.massageSql(queryParams))
              df = SparderEnv.getDF
            } catch {
              case _: SQLException =>
                val afterConvert = QueryUtil.massagePushDownSql(queryParams)
                // Table schema comes from csv and DATABASE.TABLE is not supported.
                val sqlForSpark = removeDataBaseInSql(afterConvert)
                df = spark.sql(sqlForSpark)
            }

            val result = sortedDF(df).take(100).map(row => row.toString).mkString(":")
              .replace("\r", "\\r").replace("\n", "\\n")

            if (dumpResult) {
              dumpWriter.println(result)
              dumpWriter.flush()
            } else {
              assert(result == expctedResult.next(), s"sql$idx failed")
            }
          }
          idx += 1
        })
      } finally {
        expctedResultSource.close()
        dumpWriter.close()
      }
    }
  }

  def build(): Unit = logTime("Build Time: ", debug = true) {
    val AUTO_MODEL_STAPLES_1 = "e31343d1-d921-4d61-83ec-5510b35a0409"
    val AUTO_MODEL_CALCS_1 = "ee6d8257-684f-4dcb-b531-5dbf2b6ffd32"
    val models = Array(AUTO_MODEL_STAPLES_1, AUTO_MODEL_CALCS_1)

    models.foreach(id => fullBuildCube(id))

    // replace metadata with new one after build
    dumpMetadata()
  }

  def removeDataBaseInSql(originSql: String): String = {
    originSql
      .replaceAll("(?i)TDVT\\.", "") //
      .replaceAll("\"TDVT\"\\.", "") //
      .replaceAll("`TDVT`\\.", "")
  }

  def isEmpty(sql: String): Boolean = {
    var i = 0
    for (i <- 0 until sql.length) {
      if (!(sql.charAt(i) == '\r' ||
        sql.charAt(i) == '\n' ||
        sql.charAt(i) == '\t' ||
        sql.charAt(i) == ' ')) {
        return false
      }
    }
    true
  }

  def sortedDF(df: DataFrame): DataFrame = {
    val indexedCols = df.columns.indices.map(i => s"col$i")
    val indexedDf = df.toDF(indexedCols: _*)
    val castedCols = indexedDf.schema.fields.map(field => {
      field.dataType match {
        case DoubleType | FloatType =>
          new Column(field.name).cast("decimal(32,2)").alias(field.name)
        case _ =>
          new Column(field.name)
      }
    })
    indexedDf.select(castedCols: _*).sort(indexedCols.map(i => new Column(i)): _*)
  }
}
