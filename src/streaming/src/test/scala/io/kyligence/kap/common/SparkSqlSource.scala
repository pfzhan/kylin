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

import java.io.File

import io.kyligence.kap.metadata.cube.model.{NDataflow, NDataflowManager}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TableDesc
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.JavaConverters._

trait SparkSqlSource {


  def registerSSBTable(spark: SparkSession, project: String, dataflowId: String): Unit = {

    val config = KylinConfig.getInstanceFromEnv
    val dfMgr: NDataflowManager = NDataflowManager.getInstance(config, project)
    var df: NDataflow = dfMgr.getDataflow(StreamingTestConstant.DATAFLOW_ID)
    registSSBTable(config, spark, df.getModel.getRootFactTable.getTableDesc)
    val joinTables = df.getModel.getJoinTables
    joinTables.asScala.foreach { joinDesc =>
      registSSBTable(config, spark, joinDesc.getTableRef.getTableDesc)
    }

  }


  def registSSBTable(config: KylinConfig, ss: SparkSession, tableDesc: TableDesc): Unit = {

    val schema =
      StructType(
        tableDesc.getColumns.filter(!_.getName.equals("LO_MINUTES")).map { columnDescs =>
          StructField(columnDescs.getName, SparderTypeUtil.toSparkType(columnDescs.getType, false))
        }
      )
    val utMetaDir = System.getProperty(KylinConfig.KYLIN_CONF)
    val path = new File(utMetaDir, "data/" + tableDesc.getDatabase + "." + tableDesc.getName + ".csv").getAbsolutePath
    ss.read.option("delimiter", ",").schema(schema).csv(path).createOrReplaceGlobalTempView(tableDesc.getName)
  }


}
