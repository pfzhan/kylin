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

import java.sql.SQLException
import java.util.Optional

import io.kyligence.kap.engine.spark.utils.JavaOptionals._
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataflow}
import io.kyligence.kap.secondstorage.enums.LockTypeEnum
import io.kyligence.kap.secondstorage.metadata._
import org.apache.kylin.common.{ForceToTieredStorage, KylinConfig, QueryContext}
import org.apache.spark.sql.datasource.storage.StorageStoreFactory
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.util.control.NonFatal

object SecondStorage extends LogEx {

  private[secondstorage] def load(pluginClassName: String): SecondStoragePlugin = {
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
      case e: ClassNotFoundException =>
        logWarning(s"ClassNotFoundException", e)
        null
    }
  }

  private var secondStoragePlugin: SecondStoragePlugin = _

  lazy val configLoader: SecondStorageConfigLoader = {
    if (secondStoragePlugin == null) {
      throw new RuntimeException("second storage plugin is null")
    }
    secondStoragePlugin.getConfigLoader
  }

  def init(force: Boolean): Unit = {
    if (force || secondStoragePlugin == null) {
      secondStoragePlugin = Option(KylinConfig.getInstanceFromEnv.getSecondStorage).map(load).orNull
    }
  }

  def enabled: Boolean = secondStoragePlugin != null && secondStoragePlugin.ready()

  def tableFlowManager(config: KylinConfig, project: String): Manager[TableFlow] =
    secondStoragePlugin.tableFlowManager(config, project)

  def tableFlowManager(dataflow: NDataflow): Manager[TableFlow] =
    tableFlowManager(dataflow.getConfig, dataflow.getProject)

  def tablePlanManager(config: KylinConfig, project: String): Manager[TablePlan] =
    secondStoragePlugin.tablePlanManager(config, project)

  def nodeGroupManager(config: KylinConfig, project: String): Manager[NodeGroup] =
    secondStoragePlugin.nodeGroupManager(config, project)

  private def queryCatalog() = Option.apply(secondStoragePlugin.queryCatalog())

  def trySecondStorage(
                        sparkSession: SparkSession,
                        dataflow: NDataflow,
                        layout: LayoutEntity,
                        pruningInfo: String): Option[DataFrame] = {
    if (!SecondStorageUtil.isModelEnable(dataflow.getProject, dataflow.getModel.getId)) {
      return None
    }

    if (LockTypeEnum.locked(LockTypeEnum.QUERY.name(), SecondStorageUtil.getProjectLocks(dataflow.getProject))) {
      log.info("project={} has 'QUERY' lock, can not hit clickhouse query.", dataflow.getProject)
      return Option.empty
    }

    var result = Option.empty[DataFrame]
    // Only support table index
    val enableSSForThisQuery = enabled && layout.getIndex.isTableIndex && !QueryContext.current().isForceTableIndex
    val allSegIds = pruningInfo.split(",").map(s => s.split(":")(0)).toSet.asJava
    while (enableSSForThisQuery && result.isEmpty && QueryContext.current().isRetrySecondStorage) {
      val tableData = Option.apply(enableSSForThisQuery)
        .filter(_ == true)
        .flatMap(_ =>
          tableFlowManager(dataflow)
            .get(dataflow.getUuid)
            .flatMap(f => f.getEntity(layout))
            .toOption)
        .filter { tableData =>
          tableData.containSegments(allSegIds)
        }
      if (tableData.isEmpty) {
        QueryContext.current().setRetrySecondStorage(false)
        logInfo("No table data found.")
      }
      result = tableData.flatMap(tableData =>
        tryCreateDataFrame(Some(tableData), sparkSession, dataflow, allSegIds)
      )
    }
    if (result.isDefined) {
      QueryContext.current().setLastFailed(false)
      QueryContext.current().getSecondStorageUsageMap.put(layout.getId, true)
    }

    result
  }

  private def tryCreateDataFrame(tableData: Option[TableData], sparkSession: SparkSession,
                                 dataflow: NDataflow, allSegIds: java.util.Set[String]) = {
    try {
      var project = dataflow.getProject
      for {
        shardJDBCURLs <- tableData.map(_.getShardJDBCURLs(project, allSegIds))
        database <- tableData.map(_.getDatabase)
        table <- tableData.map(_.getTable)
        catalog <- queryCatalog()
      } yield {
        sparkSession.read
          .option(ShardOptions.SHARD_URLS, shardJDBCURLs)
          .table(s"$catalog.$database.$table")
      }
    } catch {
      case NonFatal(e) =>
        logDebug("Failed to use second storage table-index", e)
        val forcedToTieredStorage = Optional.ofNullable(QueryContext.current.getForcedToTieredStorage).orElse(ForceToTieredStorage.CH_FAIL_TO_DFS)
        if (forcedToTieredStorage != ForceToTieredStorage.CH_FAIL_TO_DFS) {
          throw new SQLException(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE)
        }
        QueryContext.current().setLastFailed(true);
        None
    }
  }
}
