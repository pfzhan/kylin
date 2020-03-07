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

package org.apache.spark.sql.datasource.storage

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.FileFormatWriter.OutputSpec
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, FileFormatWriter}
import org.apache.spark.sql.{Row, SparkSession}

case class UnsafelyInsertIntoHadoopFsRelationCommand(
                                                      qualifiedOutputPath: Path,
                                                      query: LogicalPlan,
                                                      table: CatalogTable,
                                                      extractPartitionDirs: Set[String] => Unit
                                                    ) extends DataWritingCommand {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val outAttributes = query.output
    logInfo(s"Write data to $qualifiedOutputPath.")
    val partitionDirs = FileFormatWriter.write(
      sparkSession,
      child,
      DataSource.lookupDataSource("parquet", sparkSession.sessionState.conf).newInstance().asInstanceOf[FileFormat],
      FileCommitProtocol.instantiate(
        sparkSession.sessionState.conf.fileCommitProtocolClass,
        jobId = java.util.UUID.randomUUID().toString,
        outputPath = qualifiedOutputPath.toString),
      OutputSpec(
        qualifiedOutputPath.toString, Map.empty, outAttributes),
      hadoopConf,
      outAttributes.filter(a => table.partitionColumnNames.contains(a.name)),
      table.bucketSpec,
      Seq(basicWriteJobStatsTracker(hadoopConf)),
      Map.empty
    )
    extractPartitionDirs(partitionDirs)
    Seq.empty
  }

  override def outputColumnNames: Seq[String] = query.output.map(_.name)
}
