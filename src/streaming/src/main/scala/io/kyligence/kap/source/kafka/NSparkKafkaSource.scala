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
package io.kyligence.kap.source.kafka

import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.source.NSparkMetadataExplorer
import org.apache.commons.lang.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.{IBuildable, SegmentRange, TableDesc}
import org.apache.kylin.source.{IReadableTable, ISampleDataDeployer, ISource, ISourceMetadataExplorer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.Map
import java.util.concurrent.ArrayBlockingQueue
import scala.io.Source


class NSparkKafkaSource(val kylinConfig: KylinConfig) extends ISource {
  private val textFileQueue = new ArrayBlockingQueue[String](1000)
  private var msEvents: MemoryStream[Row] = null
  private var memoryStreamEnabled = false

  // scalastyle:off

  /**
   * Return an explorer to sync table metadata from the data source.
   */
  override def getSourceMetadataExplorer: ISourceMetadataExplorer = {
    new KafkaExplorer()
  }

  /**
   * Return an adaptor that implements specified interface as requested by the build engine.
   * The IMRInput in particular, is required by the MR build engine.
   */
  override def adaptToBuildEngine[I](engineInterface: Class[I]): I = {
    if (engineInterface eq classOf[NSparkCubingEngine.NSparkCubingSource]) {
      val source = new NSparkCubingEngine.NSparkCubingSource() {
        override def getSourceData(table: TableDesc, ss: SparkSession, parameters: Map[String, String]): Dataset[Row] = {
          if (KylinConfig.getInstanceFromEnv.isUTEnv) {
            val schema = new StructType().add("value", StringType)
            if (memoryStreamEnabled) {
              msEvents = MemoryStream[Row](1)(RowEncoder(schema), ss.sqlContext)
              val text = Source.fromFile(textFileQueue.take())
              msEvents.addData(Row(text.getLines().mkString))
              msEvents.toDS()
            } else {
              val rdd = ss.read.text(textFileQueue.take()).rdd
              val dataframe = ss.createDataFrame(rdd, schema)
              dataframe.as[Row](RowEncoder(schema))
            }
          } else {
            ss.readStream.format("kafka").options(parameters).load()
          }
        }
      }
      return source.asInstanceOf[I]
    }
    throw new IllegalArgumentException("Unsupported engine interface: " + engineInterface)
  }

  /**
   * Return a ReadableTable that can iterate through the rows of given table.
   */
  override def createReadableTable(tableDesc: TableDesc): IReadableTable = {
    throw new UnsupportedOperationException
  }

  /**
   * Give the source a chance to enrich a SourcePartition before build start.
   * Particularly, Kafka source use this chance to define start/end offsets within each partition.
   */
  override def enrichSourcePartitionBeforeBuild(buildable: IBuildable, segmentRange:
  SegmentRange[_ <: Comparable[_]]): SegmentRange[_ <: Comparable[_]] = {
    throw new UnsupportedOperationException
  }

  /**
   * Return an object that is responsible for deploying sample (CSV) data to the source database.
   * For testing purpose.
   */
  override def getSampleDataDeployer: ISampleDataDeployer = {
    throw new UnsupportedOperationException
  }

  override def getSegmentRange(start: String, end: String): SegmentRange[_ <: Comparable[_]] = {
    var startTime = start
    var endTime = end
    if (StringUtils.isEmpty(start)) startTime = "0"
    if (StringUtils.isEmpty(end)) endTime = "" + Long.MaxValue
    new SegmentRange.KafkaOffsetPartitionedSegmentRange(startTime.toLong, endTime.toLong)
  }

  def enableMemoryStream(): Boolean = {
    this.memoryStreamEnabled
  }

  def enableMemoryStream(mse: Boolean): Unit = {
    this.memoryStreamEnabled = mse
  }

  def post(textFile: String): Unit = {
    if (msEvents != null && memoryStreamEnabled) {
      val text = Source.fromFile(textFile)
      msEvents.addData(Row(text.getLines().mkString))
    } else {
      textFileQueue.offer(textFile)
    }
  }

  override def supportBuildSnapShotByPartition = true

  // scalastyle:on
}
