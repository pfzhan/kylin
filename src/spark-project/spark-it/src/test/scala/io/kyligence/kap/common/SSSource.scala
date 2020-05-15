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

package io.kyligence.kap.common

import com.google.common.base.Preconditions
import io.kyligence.kap.metadata.model.NTableMetadataManager
import io.kyligence.kap.query.util.SparkSQLFunctionConverter
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.query.util.{QueryParams, QueryUtil}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession}
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.scalatest.Suite

import scala.util.Random

trait SSSource extends SharedSparkSession with LocalMetadata {
  self: Suite =>

  val CSV_TABLE_DIR = "../examples/test_metadata/data/%s.csv"

  override def beforeAll() {
    super.beforeAll()
    val project = "default"
    import io.kyligence.kap.metadata.project.NProjectManager
    val kylinConf = KylinConfig.getInstanceFromEnv
    val projectInstance =
      NProjectManager.getInstance(kylinConf).getProject(project)
    Preconditions.checkArgument(projectInstance != null)
    import scala.collection.JavaConverters._
    projectInstance.getTables.asScala
      .filter(!_.equals("DEFAULT.STREAMING_TABLE"))
      .foreach { table =>
        val tableDesc = NTableMetadataManager
          .getInstance(kylinConf, project)
          .getTableDesc(table)
        val columns = tableDesc.getColumns
        val schema = SchemaProcessor.buildSchemaWithRawTable(columns)
        var tableN = tableDesc.getName
        if (table.equals("DEFAULT.TEST_KYLIN_FACT")) {
          tableN = tableDesc.getName + "_table"
        }
        spark.catalog.createTable(
          tableName = tableN,
          source = "csv",
          schema = schema,
          options = Map("path" -> String.format(CSV_TABLE_DIR, table)))
        if (table.equals("DEFAULT.TEST_KYLIN_FACT")) {
          spark.sql("create view " + tableDesc.getName + " as select * from " + tableN)
        }
      }
  }

  def cleanSql(originSql: String): String = {
    val sqlForSpark = originSql
      .replaceAll("edw\\.", "")
      .replaceAll("\"EDW\"\\.", "")
      .replaceAll("EDW\\.", "")
      .replaceAll("default\\.", "")
      .replaceAll("DEFAULT\\.", "")
      .replaceAll("\"DEFAULT\"\\.", "")
    val queryParams = new QueryParams("default", sqlForSpark, "DEFAULT", false)
    QueryUtil.massagePushDownSql(queryParams)
  }
}
