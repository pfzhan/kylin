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
package org.apache.spark.sql.execution.datasources.jdbc.v2

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTableCatalog
import org.apache.spark.sql.types.{BooleanType, StructType}

import io.kyligence.kap.engine.spark.utils.JavaOptionals.toRichOptional
import io.kyligence.kap.metadata.cube.model.NDataflowManager
import io.kyligence.kap.secondstorage.ColumnMapping.secondStorageColumnToKapColumn
import io.kyligence.kap.secondstorage.NameUtil
import io.kyligence.kap.secondstorage.SecondStorage.tableFlowManager

class SecondStorageCatalog extends ShardJDBCTableCatalog {

  protected val cache: LoadingCache[Identifier, StructType] = CacheBuilder.newBuilder()
    .maximumSize(100L)
    .build(
      new CacheLoader[Identifier, StructType]() {
        override def load(ident: Identifier): StructType = {
          val config = KylinConfig.getInstanceFromEnv
          val project = NameUtil.recoverProject(ident.namespace()(0), config)
          val pair = NameUtil.recoverLayout(ident.name())
          val model_id = pair.getFirst
          val layout = pair.getSecond
          val dfMgr = NDataflowManager.getInstance(config, project)
          val orderedDims = dfMgr.getDataflow(model_id).getIndexPlan.getLayoutEntity(layout).getOrderedDimensions

          val schemaURL = tableFlowManager(config, project)
            .get(model_id)
            .flatMap(f => f.getEntity(layout))
            .toOption
            .map(_.getSchemaURL)
            .get

          val immutable = Map(
            JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident),
            JDBCOptions.JDBC_URL -> schemaURL)
          val jdbcSchema = JDBCRDD.resolveTable(new JDBCOptions(immutable))

          // !!This is patch!!
          StructType(jdbcSchema.map(e => {
            orderedDims.get(secondStorageColumnToKapColumn(e.name).toInt).getType.getName match {
              case DataType.BOOLEAN => e.copy(dataType = BooleanType)
              case _ => e
            }
          })
          )
        }
      })

  override protected def resolveTable(ident: Identifier): Option[StructType] = {
    Option.apply(cache.get(ident))
  }
}
