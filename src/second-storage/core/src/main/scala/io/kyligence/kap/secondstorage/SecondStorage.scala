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
package io.kyligence.kap.secondstorage

import io.kyligence.kap.engine.spark.utils.JavaOptionals._
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataflow}
import io.kyligence.kap.secondstorage.metadata.{NManager, NodeGroup, TableFlow, TablePlan}
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, ShardOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters
import scala.util.Random

object SecondStorage extends LogEx {

  private [secondstorage] def load(pluginClassName: String): SecondStoragePlugin = {
    try {
      // scalastyle:off classforname
      val pluginClass =
        Option.apply(Class.forName(pluginClassName, true, Thread.currentThread.getContextClassLoader))
      // scalastyle:on classforname

      pluginClass
        .flatMap { p =>
          if (!classOf[SecondStoragePlugin].isAssignableFrom(p)) {
            logWarning(s"SecondStoragePlugin plugin class not found: $pluginClassName is not defined")
            Option(null)
          } else {
            Some(p.getDeclaredConstructor().newInstance().asInstanceOf[SecondStoragePlugin])
          }
        }
        .orNull
    } catch {
      case _: ClassNotFoundException =>
        logWarning(s"ClassNotFoundException")
        null
    }
  }

  private lazy val secondStoragePlugin: SecondStoragePlugin =
    Option(KylinConfig.getInstanceFromEnv.getSecondStorage).map(load).orNull


  def enabled: Boolean = secondStoragePlugin != null && secondStoragePlugin.ready()

  def tableFlowManager(config: KylinConfig, project: String): NManager[TableFlow] =
    secondStoragePlugin.tableFlowManager(config, project)

  def tableFlowManager(dataflow: NDataflow): NManager[TableFlow] =
    tableFlowManager(dataflow.getConfig, dataflow.getProject)

  def tablePlanManager(config: KylinConfig, project: String): NManager[TablePlan] =
    secondStoragePlugin.tablePlanManager(config, project)

  def nodeGroupManager(config: KylinConfig, project: String): NManager[NodeGroup] =
    secondStoragePlugin.nodeGroupManager(config, project)

  def trySecondStorage(
                        sparkSession: SparkSession,
                        dataflow: NDataflow,
                        layout: LayoutEntity,
                        pruningInfo: String): Option[DataFrame] = {
    // Only support table index
    if (enabled && layout.getIndex.isTableIndex) {
      val tableData = tableFlowManager(dataflow)
        .get(dataflow.getUuid)
        .flatMap(f => f.getEntity(layout))
        .toOption
      if (tableData.isDefined) {
        val allSegIds = pruningInfo.split(",").map(s => s.split(":")(0)).toSet
        val pruningSegIds = JavaConverters.asScalaBuffer(tableData.get.getPartitions).map(p => p.getSegmentId).toSet
        val missingSegs = allSegIds.diff(pruningSegIds).toList
        if (missingSegs.nonEmpty) return Option.empty
      }
      for {
        externalJDBCURL <- tableData.filter(_.getPartitions.size() > 0)
          .map(t => {
            // random choice available shards replica
            val nodes = t.getPartitions.get(Random.nextInt(t.getPartitions.size())).getShardNodes
            ShardOptions.buildSharding(JavaConverters.asScalaBuffer(SecondStorageNodeHelper.resolve(nodes)): _*)
          })
        database <- tableData.map(_.getDatabase)
        table <- tableData.map(_.getTable)
        catalog <- Option.apply(secondStoragePlugin.queryCatalog())
      } yield {
        SchemaCache.putIfAbsent(s"$database.$table", key => {
          val immutable = Map(
            JDBCOptions.JDBC_TABLE_NAME -> key,
            JDBCOptions.JDBC_URL -> ShardOptions(externalJDBCURL).shards(0))
          JDBCRDD.resolveTable(new JDBCOptions(immutable))
        })
        sparkSession.read
          .option(ShardOptions.SHARD_URLS, externalJDBCURL)
          .table(s"$catalog.$database.$table")
      }
    } else {
      Option.empty
    }
  }
}
