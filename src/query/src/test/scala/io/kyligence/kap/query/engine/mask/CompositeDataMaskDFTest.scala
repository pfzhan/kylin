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
import io.kyligence.kap.metadata.acl.{DependentColumn, DependentColumnInfo, SensitiveDataMask, SensitiveDataMaskInfo}
import io.kyligence.kap.query.engine.QueryExec
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.RandomUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll

//noinspection ScalaStyle
class CompositeDataMaskDFTest extends org.scalatest.funsuite.AnyFunSuite with BeforeAndAfterAll {

  private var mask: QueryResultMask = _
  private var ss: SparkSession = _
  private val localFileMetadata :NLocalFileMetadataTestCase = new NLocalFileMetadataTestCase

  override def beforeAll(): Unit = {
    localFileMetadata.createTestMetadata()
    val maskInfo = new SensitiveDataMaskInfo
    maskInfo.addMasks("DEFAULT", "TEST_ACCOUNT",
      Lists.newArrayList(
        new SensitiveDataMask("ACCOUNT_CONTACT", SensitiveDataMask.MaskType.DEFAULT)
      )
    )
    val sensitiveMask = new QuerySensitiveDataMask("DEFAULT", maskInfo)

    val dependentColumnInfo = new DependentColumnInfo
    dependentColumnInfo.add("DEFAULT", "TEST_KYLIN_FACT",
      Lists.newArrayList(
        new DependentColumn("PRICE", "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL", Array[String]("1", "2"))
      )
    )
    val dependentMask = new QueryDependentColumnMask("DEFAULT", dependentColumnInfo)

    mask = new CompositeQueryResultMasks(sensitiveMask, dependentMask)

    val sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr).setMaster("local[1]")
    ss = SparkSession.builder.config(sparkConf).getOrCreate
  }

  override def afterAll(): Unit = {
    ss.close()
    localFileMetadata.cleanupTestMetadata()
  }

  test("test simple") {
    val sql = "SELECT PRICE, ACCOUNT_BUYER_LEVEL, ACCOUNT_ID, ACCOUNT_CONTACT " +
      "FROM TEST_KYLIN_FACT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";

    val queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv)
    val relNode = queryExec.parseAndOptimize(sql)
    mask.doSetRootRelNode(relNode)
    mask.init()

    val df = ss.createDataFrame(
      ss.sparkContext.parallelize(
        Seq(
          Row(123, 1, 1, "foo"),
          Row(123, 4, 1, "foo")
        )
      ),
      StructType(
        List(
          StructField("PRICE", DataTypes.IntegerType),
          StructField("ACCOUNT_BUYER_LEVEL", DataTypes.IntegerType),
          StructField("ACCOUNT_ID", DataTypes.IntegerType),
          StructField("ACCOUNT_CONTACT", DataTypes.StringType)
        ))
    )
    val masked = mask.doMaskResult(df)
    assertResult(Seq(
      Row(123, 1, 1, "****"),
      Row(null, 4, 1, "****")
    )) {
      masked.collect()
    }
  }
}
