/*
 *
 *  * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *  *
 *  * http://kyligence.io
 *  *
 *  * This software is the confidential and proprietary information of
 *  * Kyligence Inc. ("Confidential Information"). You shall not disclose
 *  * such Confidential Information and shall use it only in accordance
 *  * with the terms of the license agreement you entered into with
 *  * Kyligence Inc.
 *  *
 *  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *  * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *  * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *  * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql.execution.datasources.sparder

import java.io.{IOException, ObjectInputStream}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.TimeUnit

import com.google.common.cache.{
  Cache,
  CacheBuilder,
  RemovalListener,
  RemovalNotification
}
import io.kyligence.kap.metadata.filter.TupleFilterSerializerRawTableExt
import io.kyligence.kap.storage.parquet.cube.spark.rpc.RDDPartitionResult
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos
import io.kyligence.kap.storage.parquet.cube.spark.rpc.sparder.RowToBytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.gridtable.{GTInfo, GTScanRequest, GTUtil}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.sparder.sqlgen.SqlGen
import org.apache.spark.sql.execution.utils.{
  HexUtils,
  RowTearer,
  SchemaProcessor
}
import org.apache.spark.sql.udf.{AggraFuncArgs, AggraFuncArgs2, SparderAggFun}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class SparkSparderVisit(request: SparkJobProtos.SparkJobRequestPayload,
                        sc: JavaSparkContext,
                        streamIdentifier: String)
    extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(classOf[SparkSparderVisit])

  val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate
  val realizationType: String = request.getRealizationType
  val dataFolderName: String = request.getDataFolderName
  KylinConfig.setKylinConfigInEnvIfMissing(request.getKylinProperties)
  val kylinConfig: KylinConfig = KylinConfig.getInstanceFromEnv
  val conf: Configuration = new Configuration
  val kapConfig: KapConfig = KapConfig.wrap(kylinConfig)
  TupleFilterSerializerRawTableExt.getExtendedTupleFilters //  touch static initialization
  lazy val scanRequest: GTScanRequest = GTScanRequest.serializer.deserialize(
    ByteBuffer.wrap(request.getGtScanRequest.toByteArray))
  val startTime: Long = System.currentTimeMillis
  val cuboid: Long = request.getDataFolderName.toLong
  val parquetPathIter: Seq[String] = cuboidFileSeq()
  val gtInfo: GTInfo = scanRequest.getInfo
  val gTinfoStr: String = {
    val allocate: ByteBuffer =
      ByteBuffer.allocate(request.getGtScanRequest.toByteArray.length)
    GTInfo.serializer.serialize(gtInfo, allocate)
    HexUtils.toHexString(allocate.array())
  }

  val scanRequestStr: String =
    new String(request.getGtScanRequest.toByteArray, "ISO-8859-1")

  def executeTask()
    : org.apache.kylin.common.util.Pair[util.Iterator[RDDPartitionResult],
                                        JavaRDD[RDDPartitionResult]] = {
    val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
    spark.read
      .format(
        "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
      .option(SparderConstants.FILTER_PUSH_DOWN,
              HexUtils.toHexString(
                GTUtil.serializeGTFilter(scanRequest.getFilterPushDown,
                                         scanRequest.getInfo)))
      .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
      .option(SparderConstants.CUBOID_ID, cuboid)
      .schema(SchemaProcessor.buildSchemaV2(scanRequest.getInfo, tableName))
      .load(parquetPathIter: _*)
      .createOrReplaceTempView(tableName)
    val boardcastGTInfo = spark.sparkContext.broadcast(gTinfoStr)
    val cleanMap = cleanRequest(scanRequest, gTinfoStr)
    val intToString = register(cleanMap, spark, tableName)
    logger.info(intToString.toString())

    val partitions = spark.sql(SqlGen.genSql(scanRequest, intToString)).javaRDD
    val collect =
      partitions.repartition(1).mapPartitions(new RowToBytes()).cache
    val results = collect.collect
    new org.apache.kylin.common.util.Pair(results.iterator(), collect)
    //      .rdd.mapPartitions{
    //      iterator =>{
    //        transform(iterator)
    //      }
    //    })
  }

  def cleanRequest(gTScanRequest: GTScanRequest,
                   gTinfoStr: String): Map[Int, AggraFuncArgs] = {

    gTScanRequest.getAggrMetrics
      .iterator()
      .asScala
      .zip(gTScanRequest.getAggrMetricsFuncs.iterator)
      .map {
        case (colId, funName) =>
          (colId.toInt,
           AggraFuncArgs(funName,
                         gTScanRequest.getInfo.getColumnType(colId),
                         gTinfoStr))
      }
      .toMap
  }

  def register(aggrMap: Map[Int, AggraFuncArgs],
               sparkSession: SparkSession,
               tableName: String): Map[Int, String] = {
    aggrMap.map {
      case (colId, aggr) =>
        val name = aggr.dataTp.toString
          .replace("(", "_")
          .replace(")", "_")
          .replace(",", "_") + aggr.funcName

        spark.udf.register(
          name,
          new SparderAggFun(AggraFuncArgs2(aggr.funcName, aggr.dataTp)))
        (colId, name)
    }
  }

  def cuboidFileSeq(): Seq[String] = {
    val workingDir = kylinConfig.getHdfsWorkingDirectory
    val SCHEMA_HINT = "://"
    val cubeMapping: util.Map[Long, util.Set[String]] = readCubeMappingInfo
    val parquetPathCollection: util.Set[String] = cubeMapping.get(cuboid)

    parquetPathCollection.asScala.map { path =>
      if (path.contains(SCHEMA_HINT)) {
        path
      } else { workingDir + "/" + path }
    }.toSeq
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readCubeMappingInfo: util.Map[Long, util.Set[String]] = {
    //scalastyle:off
    val cubeInfoPath = new StringBuilder(kylinConfig.getHdfsWorkingDirectory)
      .append("parquet/")
      .append(request.getRealizationId)
      .append("/")
      .append(request.getSegmentId)
      .append("/")
      .append("CUBE_INFO")
      .toString
    val ifPresent =
      SparkSparderVisit.cubeMappingCache.getIfPresent(cubeInfoPath)
    if (ifPresent != null) return ifPresent
    logger.info("not hit cube mapping")
    val fs = FileSystem.get(conf)
    if (fs.exists(new Path(cubeInfoPath))) {
      var map: util.Map[Long, util.Set[String]] = null
      val inputStream = new ObjectInputStream(fs.open(new Path(cubeInfoPath)))
      try {
        map =
          inputStream.readObject.asInstanceOf[util.Map[Long, util.Set[String]]]

        SparkSparderVisit.cubeMappingCache.put(cubeInfoPath, map)
      } finally if (inputStream != null) inputStream.close()
      map
    } else throw new RuntimeException("Cannot find CubeMappingInfo")
  }

}

object SparkSparderVisit {
  val logger: Logger = LoggerFactory.getLogger(classOf[SparkSparderVisit])

  val cubeMappingCache
    : Cache[String, util.Map[Long, util.Set[String]]] = CacheBuilder.newBuilder
    .maximumSize(10000)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .removalListener(
      new RemovalListener[String, util.Map[Long, util.Set[String]]]() {

        override def onRemoval(
            notification: RemovalNotification[String,
                                              util.Map[Long, util.Set[String]]])
          : Unit = {
          logger.info(
            String.format(
              "cubeMappingCache entry with key %s is removed due to %s ",
              notification.getKey,
              notification.getCause.toString))
        }
      })
    .build
    .asInstanceOf[Cache[String, util.Map[Long, util.Set[String]]]]

  val cubePathCache: Cache[String, util.Set[String]] = CacheBuilder.newBuilder
    .maximumSize(10000)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .removalListener(new RemovalListener[String, util.Set[String]]() {
      override def onRemoval(
          notification: RemovalNotification[String, util.Set[String]]): Unit = {
        logger.info(
          String.format("cubePathCache entry with key %s is removed due to %s ",
                        notification.getKey,
                        notification.getCause.toString))
      }
    })
    .build
    .asInstanceOf[Cache[String, util.Set[String]]]
  var spark: SparkSession = _

  def apply(request: SparkJobProtos.SparkJobRequestPayload,
            sc: JavaSparkContext,
            streamIdentifier: String): SparkSparderVisit =
    new SparkSparderVisit(request, sc, streamIdentifier)

  def initSparkSession(sc: JavaSparkContext): Unit = {
    spark = SparkSession.builder.config(sc.getConf).getOrCreate
  }

  def initSparkSession(sc: SparkSession): Unit = {
    spark = sc
  }

}
