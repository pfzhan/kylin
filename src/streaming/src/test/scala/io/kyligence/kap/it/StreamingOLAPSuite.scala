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
package io.kyligence.kap.it
/*
import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import io.kyligence.kap.common._
import io.kyligence.kap.metadata.cube.model.{NDataflow, NDataflowManager}
import io.kyligence.kap.metadata.cube.utils.StreamingUtils
import io.kyligence.kap.query.QueryFetcher
import io.kyligence.kap.streaming.app.{StreamingEntry, StreamingMergeEntry}
import io.kyligence.kap.streaming.jobs.StreamingSegmentManager
import org.apache.hadoop.conf.Configuration
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.ZKUtil
import org.apache.kylin.job.execution.JobTypeEnum
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite, SparderQueryTest}
import org.apache.spark.sql.kafka010.OffsetRangeManager
import org.apache.spark.utils.KafkaTestUtils
import org.apache.zookeeper.KeeperException
import org.scalatest.concurrent.Eventually

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

/**
  * set working dir to "your_path/KAP/src/streaming/examples"
  */
class StreamingOLAPSuite
  extends SparderBaseFunSuite
    with QuerySupport
    with StreamingJobSupport
    with SparkSqlSource
    with LocalMetadata
    with Eventually
    with StreamingDataGenerate
    with KafkaTestUtils {

  override def beforeAll(): Unit = {

    super.beforeAll()
    setup()
    val hdfsLocalCluster = new HdfsLocalCluster.Builder()
      .setHdfsNamenodePort(8020)
      .setHdfsNamenodeHttpPort(12341)
      .setHdfsTempDir("embedded_hdfs")
      .setHdfsNumDatanodes(1)
      .setHdfsEnablePermissions(false)
      .setHdfsFormat(true)
      .setHdfsEnableRunningUserAsProxyUser(true)
      .setHdfsConfig(new Configuration())
      .build();

    hdfsLocalCluster.start();
  }

  test("createRepeatEphemeralPath") {
    val config = KylinConfig.getInstanceFromEnv
    config.setProperty("kylin.env.zookeeper-max-retries", "1")
    ZKUtil.createEphemeralPath("/kylin/streaming/jobs/test_query" + "_"
      + StreamingUtils.getJobId("e78a89dd-847f-4574-8afa-8768b4228b72", JobTypeEnum.STREAMING_BUILD.name), config)

    try ZKUtil.createEphemeralPath("/kylin/streaming/jobs/test_query" + "_"
      + StreamingUtils.getJobId("e78a89dd-847f-4574-8afa-8768b4228b72", JobTypeEnum.STREAMING_BUILD.name), config)
    catch {
      case e: Exception =>
        assert(e.isInstanceOf[KeeperException.NodeExistsException])
    }
  }

  test("ssb streaming query") {
    val topic = "ssb-topic"
    createTopic(topic, 1)

    val config = KylinConfig.getInstanceFromEnv
    config.setProperty("kylin.engine.streaming-base-checkpoint-location", StreamingTestConstant.CHECKPOINT_LOCATION)
    config.setProperty("kylin.engine.streaming-duration", StreamingTestConstant.INTERVAL.toString)
    config.setProperty("kylin.server.streaming-change-meta", "true")

    Future {
      StreamingEntry.main(Array[String](StreamingTestConstant.PROJECT, StreamingTestConstant.DATAFLOW_ID))
    }

    val activeQuery = waitQueryReady()

    Future {
      generate(StreamingTestConstant.KAP_SSB_STREAMING_TABLE, topic, StreamingTestConstant.KAFKA_QPS / 5, false)
    }

    Future {
      config.setProperty("kylin.job.scheduler.poll-interval-second", "1")
      StreamingMergeEntry.main(Array(StreamingTestConstant.PROJECT, StreamingTestConstant.DATAFLOW_ID, "", ""))
    }
    val endOffset = OffsetRangeManager.getKafkaSourceOffset(("ssb-topic", 0, 6005))

    val streamingTimeout = 5.seconds
    OffsetRangeManager.awaitOffset(activeQuery, 0, endOffset, streamingTimeout.toMillis)

    logInfo(s"finish build all msg")
    StreamingMergeEntry.shutdown()

    registerSSBTable(spark, StreamingTestConstant.PROJECT, StreamingTestConstant.DATAFLOW_ID)

    val query = QueryFetcher.fetchQueries(StreamingTestConstant.SQL_FOLDER).foreach { case (_, sql) =>
      logInfo(s"begin test ${sql}")
      val kylinDF = singleQuery(sql, StreamingTestConstant.PROJECT)
      val sparkDF = spark.sql(cleanSql(sql))
      assert(SparderQueryTest.same(sparkDF, kylinDF))
    }

    waitQueryStop(activeQuery)

  }


  test("streaming count distinct") {

    val topic = "count-distinct"
    createTopic(topic, 1)
    changeStreamingTableSubscribe(topic, StreamingTestConstant.COUNTDISTINCT_DATAFLOWID)


    val config = KylinConfig.getInstanceFromEnv
    config.setProperty("kylin.engine.streaming-base-checkpoint-location", StreamingTestConstant.COUNTDISTINCT_CHECKPOINT_LOCATION)
    config.setProperty("kylin.engine.streaming-duration", StreamingTestConstant.INTERVAL.toString)
    config.setProperty("kylin.server.streaming-change-meta", "true")

    Future {
      StreamingEntry.main(Array[String](StreamingTestConstant.PROJECT, StreamingTestConstant.COUNTDISTINCT_DATAFLOWID))
    }

    val activeQuery = waitQueryReady()

    Future {
      generate(StreamingTestConstant.KAP_SSB_STREAMING_TABLE, topic, StreamingTestConstant.KAFKA_QPS, false)
    }

    val endOffset = OffsetRangeManager.getKafkaSourceOffset((topic, 0, 6005))

    val streamingTimeout = 10.seconds
    OffsetRangeManager.awaitOffset(activeQuery, 0, endOffset, streamingTimeout.toMillis)
    logInfo(s"finish build all msg")
    registerSSBTable(spark, StreamingTestConstant.PROJECT, StreamingTestConstant.COUNTDISTINCT_DATAFLOWID)

    val query = QueryFetcher.fetchQueries(StreamingTestConstant.COUNTDISTINCT_SQL_FOLDER).foreach { case (_, sql) =>
      logInfo(s"begin test ${sql}")
      val kylinDF = singleQuery(sql, StreamingTestConstant.PROJECT)
      val sparkDF = spark.sql(cleanSql(sql))
      assert(SparderQueryTest.same(sparkDF, kylinDF))
    }

    waitQueryStop(activeQuery)
  }


  test("test streaming merge") {

    val topic = "merge-test"
    createTopic(topic, 1)
    changeStreamingTableSubscribe(topic, StreamingTestConstant.MERGE_DATAFLOWID)

    val config = KylinConfig.getInstanceFromEnv
    config.setProperty("kylin.engine.streaming-base-checkpoint-location", StreamingTestConstant.MERGE_CHECKPOINT_LOCATION)
    config.setProperty("kylin.engine.streaming-duration", StreamingTestConstant.INTERVAL.toString)
    config.setProperty("kylin.engine.streaming-trigger-once", "true")
    config.setProperty("kylin.server.streaming-change-meta", "true")

    // trigger once
    triggerOnce()

    (0 to StreamingTestConstant.BATCH_ROUNDS).foreach { index =>
      logInfo(s"run the ${index + 1} round")
      generate(StreamingTestConstant.KAP_SSB_STREAMING_TABLE, topic, StreamingTestConstant.High_KAFKA_QPS, false)
      triggerOnce()
    }

    val dfMgr: NDataflowManager = NDataflowManager.getInstance(config, StreamingTestConstant.PROJECT)

    assert(dfMgr.getDataflow(StreamingTestConstant.MERGE_DATAFLOWID).getSegments.size() == 6)

    generate(StreamingTestConstant.KAP_SSB_STREAMING_TABLE, topic, StreamingTestConstant.High_KAFKA_QPS, false)
    // trigger merge
    triggerOnce()

    val allThread = Thread.getAllStackTraces
    allThread.asScala.foreach { case (thread, _) =>
      val threadName = thread.getName
      if (threadName.equals("merge-thread")) {
        thread.join()
      }
    }
    assert(dfMgr.getDataflow(StreamingTestConstant.MERGE_DATAFLOWID).getSegments.size() == 2)
  }

  def changeStreamingTableSubscribe(subscribe: String, dataflowId: String): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val dfMgr: NDataflowManager = NDataflowManager.getInstance(config, StreamingTestConstant.PROJECT)
    var df: NDataflow = dfMgr.getDataflow(dataflowId)
    df.getModel.getRootFactTable.getTableDesc.getKafkaConfig.setSubscribe(subscribe)
  }

  def cleanSql(raw: String): String = {
    return raw.replace("SSB", "global_temp").stripSuffix(";")
  }

  def triggerOnce(): Unit = {
    StreamingEntry.main(Array[String](StreamingTestConstant.PROJECT, StreamingTestConstant.MERGE_DATAFLOWID))
  }


}

 */