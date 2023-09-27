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
package org.apache.spark.sql.hive.execution

import io.kyligence.kap.cache.kylin.KylinCacheFileSystem
import io.kyligence.kap.cache.softaffinity.SoftAffinityConstants
import io.kyligence.kap.softaffinity.SoftAffinityManager
import io.kyligence.kap.softaffinity.scheduler.SoftAffinityListener
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.InputSplitWithLocationInfo
import org.apache.kylin.guava30.shaded.common.io.Resources
import org.apache.spark.rdd.{HadoopPartition, RDD}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkConf}

import java.io.File
import java.net.URI

// scalastyle:off println

class HiveSoftAffinityAndLocalCacheTest extends SharedSparkSession {

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    FileSystem.closeAll()
  }

  protected override def sparkConf: SparkConf = super.sparkConf
    .set(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
    .set(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED_FOR_HIVE, "true")
    .set(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_REPLICATIONS_NUM, "2")
    .set(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_MIN_TARGET_HOSTS, "2")
    .set("spark.hadoop.fs.file.impl", "io.kyligence.kap.cache.kylin.KylinCacheFileSystem")
    .set("spark.hadoop.spark.kylin.local-cache.enabled", "true")
    .set("spark.hadoop.spark.kylin.file-status-cache.enabled", "true")
    .set("spark.hadoop.spark.kylin.local-cache.use.legacy.file-input-stream", "true")
    .set("spark.hadoop.alluxio.user.client.cache.store.type", "MEM")
    .set("spark.hadoop.alluxio.user.client.cache.size", "100MB")



  test("Test soft affinity") {
    // ASSERT CONF & INITIAL STATE
    assert(SoftAffinityManager.usingSoftAffinity)
    assert(SoftAffinityManager.usingSoftAffinityForHivePushdown)
    assert(SoftAffinityManager.totalExecutors() == 0)

    // ADD ONE EXECUTOR
    val executorsListener = new SoftAffinityListener()
    val addEvent0 = SparkListenerExecutorAdded(System.currentTimeMillis(), "0",
      new ExecutorInfo("host-1", 3, null))
    executorsListener.onExecutorAdded(addEvent0)
    assert(SoftAffinityManager.totalExecutors() == 1)

    // PREP
    val attrName = AttributeReference("name", StringType)()
    val attrAge = AttributeReference("age", LongType)()
    val attrBirthDay = AttributeReference("birth_day", DateType)()
    val attrUpdateTime = AttributeReference("update_time", TimestampType)()
    val attrs = Seq(attrName, attrAge, attrBirthDay, attrUpdateTime)

    val storageLoc = CatalogStorageFormat(
      Option(new URI("file://" +
        new File(Resources.getResource("default_3ywc6z78.emp").getPath).getAbsolutePath)),
      Option("io.kyligence.kap.cache.softaffinity.SoftAffinityTextInputFormat"), // <---- enable soft affinity for hive pushdown
      Option("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
      Option("io.kyligence.hive.serde2.lazy.LazyQuoteAwareSerDe"), // <---- enable read CSV file with "quote"
      compressed = false,
      Map("escape.delim" -> "\\", "field.delim" -> ",")
    )

    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", LongType),
      StructField("birth_day", DateType),
      StructField("update_time", TimestampType),
    ))

    val tableMeta = CatalogTable(
      TableIdentifier("emp", Option("default_3ywc6z78")),
      CatalogTableType.EXTERNAL,
      storageLoc,
      schema,
      Option("hive"),
      properties = Map("classification" -> "csv", "datasource_type" -> "0", "skip.header.line.count" -> "1")
    )

    val relation = HiveTableRelation(tableMeta, attrs, Seq())
    val enc = RowEncoder(schema).resolveAndBind()
    val des = enc.createDeserializer()

    val fs = FileSystem.get(new URI("file:///"), spark.sessionState.newHadoopConf()).asInstanceOf[KylinCacheFileSystem]
    KylinCacheFileSystem.setAcceptCacheTimeLocally(System.currentTimeMillis())

    // QUERY ROUND 1
    {
      // HIVE RDD
      val exec = HiveTableScanExec(attrs, relation, Seq())(spark)
      val rdd = exec.execute.map(r => des.apply(r))

      // PRINT PREFERRED LOCATIONS
      val parts = classOf[RDD[Row]].getDeclaredMethod("getPartitions").invoke(rdd).asInstanceOf[Array[Partition]]
      for (i <- parts.indices) {
        val fileSplit = parts(i).asInstanceOf[HadoopPartition].inputSplit.value.asInstanceOf[InputSplitWithLocationInfo]
        println("partition " + i + " locations: " + fileSplit.getLocationInfo.toStream.map(l => l.getLocation).mkString)
      }

      // PRINT DATA
      val dataset = spark.createDataset(rdd)(enc)
      dataset.show()

      // ASSERT SOFT AFFINITY HAPPENED
      val records = SoftAffinityManager.auditAsks()
      assert(records.size() == 1)
      assert(records.keySet().iterator().next().endsWith("default_3ywc6z78.emp"))
      assert(records.containsValue("executor_host-1_0"))

      // ASSERT STATE OF CACHE
      assert(fs.countCachedFilePages() == 2)
      assert(fs.countCachedFileStatus() == 4)
    }

    // QUERY ROUND 2, NOTE CACHE SHOULD STAY
    {
      val exec = HiveTableScanExec(attrs, relation, Seq())(spark)
      val rdd = exec.execute.map(r => des.apply(r))
      val dataset = spark.createDataset(rdd)(enc)
      dataset.show()

      // ASSERT SOFT AFFINITY HAPPENED
      val records = SoftAffinityManager.auditAsks()
      assert(records.size() == 1)
      assert(records.keySet().iterator().next().endsWith("default_3ywc6z78.emp"))
      assert(records.containsValue("executor_host-1_0"))

      // ASSERT STATE OF CACHE
      assert(fs.countCachedFileStatusEvictions() == 0)
      assert(fs.countCachedFilePages() == 2)
      assert(fs.countCachedFileStatus() == 4)
    }

    // CACHE HINT: REFRESH CACHE
    Thread.sleep(50)
    KylinCacheFileSystem.setAcceptCacheTimeLocally(System.currentTimeMillis())

    // QUERY ROUND 3, NOTE CACHE EVICTIONS
    {
      val exec = HiveTableScanExec(attrs, relation, Seq())(spark)
      val rdd = exec.execute.map(r => des.apply(r))
      val dataset = spark.createDataset(rdd)(enc)
      dataset.show()

      // PRINT SOFT AFFINITY LOGS
      SoftAffinityManager.logAuditAsks()

      // ASSERT STATE OF CACHE
      assert(fs.countCachedFileStatusEvictions() == 2)
      assert(fs.countCachedFilePages() == 2)
      assert(fs.countCachedFileStatus() == 4)
    }

    // QUERY ROUND 4, NOTE CACHE SHOULD STAY
    {
      val exec = HiveTableScanExec(attrs, relation, Seq())(spark)
      val rdd = exec.execute.map(r => des.apply(r))
      val dataset = spark.createDataset(rdd)(enc)
      dataset.show()

      // PRINT SOFT AFFINITY LOGS
      SoftAffinityManager.logAuditAsks()

      // ASSERT STATE OF CACHE
      assert(fs.countCachedFileStatusEvictions() == 2)
      assert(fs.countCachedFilePages() == 2)
      assert(fs.countCachedFileStatus() == 4)
    }

    // CLEANUP
    val removedEvent0 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "0", "")
    executorsListener.onExecutorRemoved(removedEvent0)
    assert(SoftAffinityManager.totalExecutors() == 0)
    KylinCacheFileSystem.clearAcceptCacheTimeLocally()
  }
}
