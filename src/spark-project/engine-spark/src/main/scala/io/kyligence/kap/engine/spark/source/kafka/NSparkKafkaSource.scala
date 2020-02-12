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
package io.kyligence.kap.engine.spark.source.kafka

import java.io.File
import java.util
import java.util.{List, Map}

import com.google.common.collect.Lists
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import org.apache.kylin.metadata.model.{ColumnDesc, IBuildable, SegmentRange, TableDesc}
import org.apache.kylin.source.{IReadableTable, ISampleDataDeployer, ISource, ISourceMetadataExplorer}
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

class NSparkKafkaSource extends ISource{
  // scalastyle:off
  /**
    * Return an explorer to sync table metadata from the data source.
    */
  override def getSourceMetadataExplorer: ISourceMetadataExplorer = ???

  /**
    * Return an adaptor that implements specified interface as requested by the build engine.
    * The IMRInput in particular, is required by the MR build engine.
    */
  override def adaptToBuildEngine[I](engineInterface: Class[I]): I =  {
    if (engineInterface eq classOf[NSparkCubingEngine.NSparkCubingSource]) {
      val source = new NSparkCubingEngine.NSparkCubingSource() {
        override def getSourceData(table: TableDesc, ss: SparkSession, parameters: util.Map[String, String]): Dataset[Row] = {
            return ss.readStream.format("kafka").options(parameters).load()
        }
      }
      return source.asInstanceOf[I]
    }
    throw new IllegalArgumentException("Unsupported engine interface: " + engineInterface)
  }

  /**
    * Return a ReadableTable that can iterate through the rows of given table.
    */
  override def createReadableTable(tableDesc: TableDesc): IReadableTable = ???

  /**
    * Give the source a chance to enrich a SourcePartition before build start.
    * Particularly, Kafka source use this chance to define start/end offsets within each partition.
    */
  override def enrichSourcePartitionBeforeBuild(buildable: IBuildable, segmentRange: SegmentRange[_ <: Comparable[_]]): SegmentRange[_ <: Comparable[_]] = ???

  /**
    * Return an object that is responsible for deploying sample (CSV) data to the source database.
    * For testing purpose.
    */
  override def getSampleDataDeployer: ISampleDataDeployer = ???

  override def getSegmentRange(start: String, end: String): SegmentRange[_ <: Comparable[_]] = ???
  // scalastyle:on
}
