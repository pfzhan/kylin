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

package org.apache.spark.sql

import java.sql.{SQLException, Timestamp}

import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataflow, NDataflowManager}
import io.kyligence.kap.metadata.model.FusionModelManager
import io.kyligence.kap.secondstorage.SecondStorage
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.datasource.storage.StorageStoreFactory
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{HashMap => MutableHashMap}

class KylinDataFrameManager(sparkSession: SparkSession) {
  private var extraOptions = new MutableHashMap[String, String]()
  private var userSpecifiedSchema: Option[StructType] = None


  /** File format for table */
  private def format(source: String): KylinDataFrameManager = {
    option("source", source)
    this
  }

  /** Add key-value to options */
  def option(key: String, value: String): KylinDataFrameManager = {
    this.extraOptions += (key -> value)
    this
  }

  /** Add boolean value to options, for compatibility with Spark */
  def option(key: String, value: Boolean): KylinDataFrameManager = {
    option(key, value.toString)
  }

  /** Add long value to options, for compatibility with Spark */
  def option(key: String, value: Long): KylinDataFrameManager = {
    option(key, value.toString)
  }

  /** Add double value to options, for compatibility with Spark */
  def option(key: String, value: Double): KylinDataFrameManager = {
    option(key, value.toString)
  }

  def isFastBitmapEnabled(isFastBitmapEnabled: Boolean): KylinDataFrameManager = {
    option("isFastBitmapEnabled", isFastBitmapEnabled.toString)
    this
  }

  def bucketingEnabled(bucketingEnabled: Boolean): KylinDataFrameManager = {
    option("bucketingEnabled", bucketingEnabled)
  }

  def cuboidTable(dataflow: NDataflow, layout: LayoutEntity, pruningInfo: String): DataFrame = {
    format("parquet")
    option("project", dataflow.getProject)
    option("dataflowId", dataflow.getUuid)
    option("cuboidId", layout.getId)
    option("pruningInfo", pruningInfo)
    if (dataflow.isStreaming && dataflow.getModel.isFusionModel) {
      val fusionModel = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv, dataflow.getProject)
              .getFusionModel(dataflow.getModel.getFusionId)
      val batchModelId = fusionModel.getBatchModel.getUuid
      val batchDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, dataflow.getProject).getDataflow(batchModelId)
      val end = batchDataflow.getDateRangeEnd

      val partition = dataflow.getModel.getPartitionDesc.getPartitionDateColumnRef
      val id = layout.getOrderedDimensions.inverse().get(partition)
      SecondStorage.trySecondStorage(sparkSession, dataflow, layout, pruningInfo).getOrElse {
        var df = StorageStoreFactory.create(dataflow.getModel.getStorageType)
          .read(dataflow, layout, sparkSession, extraOptions.toMap)
        if (end != Long.MinValue) {
          df = df.filter(col(id.toString).geq(new Timestamp(end)))
        }
        df
      }
    } else {
      SecondStorage.trySecondStorage(sparkSession, dataflow, layout, pruningInfo).getOrElse {
        StorageStoreFactory.create(dataflow.getModel.getStorageType)
          .read(dataflow, layout, sparkSession, extraOptions.toMap)
      }
    }
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   *
   * @since 1.4.0
   */
  def schema(schema: StructType): KylinDataFrameManager = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

}

object KylinDataFrameManager {
  def apply(session: SparkSession): KylinDataFrameManager = {
    new KylinDataFrameManager(session)
  }
}