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
package org.apache.spark.sql.execution.datasources.sparder.batch

import java.net.URI
import java.nio.ByteBuffer
import java.util

import com.google.common.collect.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.kylin.gridtable.GTInfo
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.sparder.{
  SparderConstants,
  SparderSchemaConverter
}
import org.apache.spark.sql.execution.datasources.{
  FileFormat,
  OutputWriterFactory,
  PartitionedFile,
  RecordReaderIterator
}
import org.apache.spark.sql.execution.utils.HexUtils
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, GTInfoSchema, SparkSession}
import org.apache.spark.util.SerializableConfiguration

class SparderBatchFileFormat
    extends FileFormat
    with DataSourceRegister
    with Serializable {

  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    Option(
      SparderBatchFileFormat.inferSchema(
        files,
        sparkSession.sparkContext.hadoopConfiguration,
        options))
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    null
  }

  override def supportBatch(sparkSession: SparkSession,
                            schema: StructType): Boolean = {
    false
  }

  override def shortName(): String = "SparderBatch"

  override def toString: String = "SparderBatch"

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO enable filter pushdown
    hadoopConf.set(SparderConstants.SPARK_PROJECT_SCHEMA, requiredSchema.json)

    //    hadoopConf.set(Config.SPARK_PROJECT_SCHEMA, requiredSchema.json)
    val conf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val resultSchema = if (requiredSchema.nonEmpty) {
      StructType(partitionSchema.fields ++ requiredSchema.fields)
    } else {
      dataSchema
    }
    (file: PartitionedFile) =>
      {

        val fileSplit =
          new FileSplit(new Path(new URI(file.filePath)),
                        file.start,
                        file.length,
                        file.locations)
        val attemptId =
          new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(conf.value.value, attemptId)
        val reader = new SparderBatchReader()
        reader.initialize(fileSplit, hadoopAttemptContext)
        reader.init(options, requiredSchema, partitionSchema, file)
        try {
          val iter = new RecordReaderIterator(reader)
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ =>
            iter.close()))
          iter.asInstanceOf[Iterator[InternalRow]]
        } catch {
          case e: Exception =>
            e.printStackTrace()
            Iterator.empty
        }
      }
  }
}

object SparderBatchFileFormat {

  def inferSchema(files: Seq[FileStatus],
                  conf: Configuration,
                  options: Map[String, String]): StructType = {
    if (options.contains(SparderConstants.KYLIN_SCAN_GTINFO_BYTES)) {
      val gtinfoString = options.apply(SparderConstants.KYLIN_SCAN_GTINFO_BYTES)
      val tableName = options.apply(SparderConstants.TABLE_ALIAS)
      val info = GTInfo.serializer.deserialize(
        ByteBuffer.wrap(HexUtils.toBytes(gtinfoString)))
      val keys = info.getPrimaryKey.iterator()
      val rowKeyStructFieldList: util.ArrayList[StructField] =
        Lists.newArrayList()
      while (keys.hasNext) {
        val key = keys.next()
        rowKeyStructFieldList.add(
          StructField(GTInfoSchema(tableName, key).toString, StringType, false))
      }
      val converter = new SparderSchemaConverter(primaryKey =
                                                   rowKeyStructFieldList.size(),
                                                 tableName = tableName)
      for (file <- files) {
        val meta: ParquetMetadata = ParquetFileReader.readFooter(
          conf,
          file.getPath,
          ParquetMetadataConverter.NO_FILTER)
        //        meta.getFileMetaData.getSchema.
        return StructType(rowKeyStructFieldList).merge(
          converter.convert(meta.getFileMetaData.getSchema))

      }
    }
    throw new AnalysisException(
      s"can not find io.kylin.storage.parquet.scan.gtinfo")
  }
}
