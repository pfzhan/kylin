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

package io.kyligence.kap.query.engine.mask

import com.google.common.collect.Lists
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase
import io.kyligence.kap.metadata.acl.{DependentColumn, DependentColumnInfo}
import io.kyligence.kap.query.engine.QueryExec
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.RandomUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll

//noinspection ScalaStyle
class QueryDependentColumnMaskDFTest extends org.scalatest.funsuite.AnyFunSuite with BeforeAndAfterAll {

  private var mask: QueryDependentColumnMask = _
  private var ss: SparkSession = _
  private val localFileMetadata :NLocalFileMetadataTestCase = new NLocalFileMetadataTestCase

  override def beforeAll(): Unit = {
    localFileMetadata.createTestMetadata()
    val dependentColumnInfo = new DependentColumnInfo
    dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT",
      Lists.newArrayList(
        new DependentColumn("PRICE", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", Array[String]("1", "2"))
      )
    )
    dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT",
      Lists.newArrayList(
        new DependentColumn("ORDER_ID", "DEFAULT.TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL", Array[String]("1", "2")),
        new DependentColumn("ORDER_ID", "DEFAULT.TEST_COUNTRY.NAME", Array[String]("China"))
      )
    )
    mask = new QueryDependentColumnMask("DEFAULT", dependentColumnInfo)
    val sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr).setMaster("local[1]")
    ss = SparkSession.builder.config(sparkConf).getOrCreate
  }

  override def afterAll(): Unit = {
    ss.close()
    localFileMetadata.cleanupTestMetadata()
  }

  test("test simple") {
    val sql = "SELECT PRICE, ACCOUNT_BUYER_LEVEL " +
      "FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";

    val queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv)
    val relNode = queryExec.parseAndOptimize(sql)
    mask.doSetRootRelNode(relNode)
    mask.init()

    val df = ss.createDataFrame(
      ss.sparkContext.parallelize(
        Seq(
          Row(123, 1),
          Row(123, 4)
        )
      ),
      StructType(
        List(
          StructField("PRICE", DataTypes.IntegerType),
          StructField("ACCOUNT_BUYER_LEVEL", DataTypes.IntegerType)
        ))
    )
    val masked = mask.doMaskResult(df)
    assertResult(Seq(
      Row(123, 1),
      Row(null, 4)
    )) {
      masked.collect()
    }
  }

  test("test multi dependent col") {
    val sql = "SELECT ORDER_ID, ACCOUNT_SELLER_LEVEL, NAME" +
      " FROM TEST_KYLIN_FACT join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID" +
      " join TEST_COUNTRY on ACCOUNT_COUNTRY = NAME"

    val queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv)
    val relNode = queryExec.parseAndOptimize(sql)
    mask.doSetRootRelNode(relNode)
    mask.init()

    val df = ss.createDataFrame(
      ss.sparkContext.parallelize(
        Seq(
          Row(123, 1, "China"),
          Row(123, 1, "CN"),
          Row(123, 4, "China")
        )
      ),
      StructType(
        List(
          StructField("ORDER_ID", DataTypes.IntegerType),
          StructField("ACCOUNT_SELLER_LEVEL", DataTypes.IntegerType),
          StructField("NAME", DataTypes.StringType)
        ))
    )
    val masked = mask.doMaskResult(df)
    assertResult(Seq(
      Row(123, 1, "China"),
      Row(null, 1, "CN"),
      Row(null, 4, "China")
    )) {
      masked.collect()
    }
  }

  test("test sum") {
    val sql = "SELECT SUM(ORDER_ID), ACCOUNT_SELLER_LEVEL, NAME" +
      " FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID" +
      " JOIN TEST_COUNTRY ON ACCOUNT_COUNTRY = NAME GROUP BY ACCOUNT_SELLER_LEVEL, NAME"

    val queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv)
    val relNode = queryExec.parseAndOptimize(sql)
    mask.doSetRootRelNode(relNode)
    mask.init()

    val df = ss.createDataFrame(
      ss.sparkContext.parallelize(
        Seq(
          Row(123, 1, "China"),
          Row(123, 1, "CN"),
          Row(123, 4, "China")
        )
      ),
      StructType(
        List(
          StructField("SUM(ORDER_ID)_&*%$#@\"'`_123", DataTypes.IntegerType),
          StructField("ACCOUNT_SELLER_LEVEL_&*%$#@\"'`._123", DataTypes.IntegerType),
          StructField("NAME", DataTypes.StringType)
        ))
    )
    val masked = mask.doMaskResult(df)
    assertResult(Seq(
      Row(123, 1, "China"),
      Row(null, 1, "CN"),
      Row(null, 4, "China")
    )) {
      masked.collect()
    }
  }

}
