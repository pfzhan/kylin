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

package io.kyligence.kap.engine.spark.job

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.builder.CreateFlatTable
import io.kyligence.kap.engine.spark.stats.analyzer.TableAnalyzerJob
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TableDesc
import org.apache.kylin.source.SourceFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, _}

class TableAnalysisJob(tableDesc: TableDesc,
                       project: String,
                       rowCount: Long,
                       ss: SparkSession) {

  def analyzeTable(): Array[Row] = {
    val config = KylinConfig.getInstanceFromEnv
    val map = config.getSparkConfigOverride

    val instances = Integer.parseInt(map.get("spark.executor.instances"))
    val cores = Integer.parseInt(map.get("spark.executor.cores"))
    val numPartitions = instances * cores * 4

    val dataset = SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, Maps.newHashMap[String, String])
      .coalesce(numPartitions)
    val rowDataset = CreateFlatTable.changeSchemaToAliasDotName(dataset, tableDesc.getIdentity)

    // todo: use sample data to estimate total info
    val ratio = rowCount / rowDataset.count().toFloat
    val sampleDataSet = if (ratio > 1) rowDataset else rowDataset.sample(ratio)

    // calculate the stats info
    val statsMetrics = buildStatsMetric(sampleDataSet)
    val aggData = sampleDataSet.agg(count(lit(1)), statsMetrics: _*).collect()

    aggData ++ sampleDataSet.limit(10).collect()
  }

  def buildStatsMetric(sourceTable: Dataset[Row]): List[Column] = {
    sourceTable.schema.fieldNames.flatMap(
      name =>
        Seq(TableAnalyzerJob.TABLE_STATS_METRICS.toArray(): _*).map {
          case "COUNT" =>
            count(col(name))
          case "COUNT_DISTINCT" =>
            approx_count_distinct(col(name))
          case "MAX" =>
            max(col(name))
          case "MIN" =>
            min(col(name))
          case _ =>
            throw new IllegalArgumentException(
              s"""Unsupported metric in TableSampling """)
        }
    ).toList
  }

}
