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

import java.util.UUID

import com.google.common.collect.Lists
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase
import io.kyligence.kap.metadata.acl.{SensitiveDataMask, SensitiveDataMaskInfo}
import io.kyligence.kap.query.engine.QueryExec
import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.BeforeAndAfterAll

//noinspection ScalaStyle
class QuerySensitiveDataMaskDFTest extends org.scalatest.funsuite.AnyFunSuite with BeforeAndAfterAll {

  private var mask: QuerySensitiveDataMask = _
  private var ss: SparkSession = _
  private val localFileMetadata :NLocalFileMetadataTestCase = new NLocalFileMetadataTestCase

  override def beforeAll(): Unit = {
    localFileMetadata.createTestMetadata()
    val maskInfo = new SensitiveDataMaskInfo
    maskInfo.addMasks("DEFAULT", "TEST_ACCOUNT",
      Lists.newArrayList(
        new SensitiveDataMask("ACCOUNT_ID", SensitiveDataMask.MaskType.DEFAULT),
        new SensitiveDataMask("ACCOUNT_BUYER_LEVEL", SensitiveDataMask.MaskType.DEFAULT),
        new SensitiveDataMask("ACCOUNT_SELLER_LEVEL", SensitiveDataMask.MaskType.AS_NULL),
        new SensitiveDataMask("ACCOUNT_CONTACT", SensitiveDataMask.MaskType.DEFAULT),
        new SensitiveDataMask("ACCOUNT_COUNTRY", SensitiveDataMask.MaskType.DEFAULT)
      )
    )
    mask = new QuerySensitiveDataMask("DEFAULT", maskInfo)

    val sparkConf = new SparkConf().setAppName(UUID.randomUUID.toString).setMaster("local[1]")
    ss = SparkSession.builder.config(sparkConf).getOrCreate
  }

  override def afterAll(): Unit = {
    ss.close()
    localFileMetadata.cleanupTestMetadata()
  }

  test("test simple") {
    val sql = "SELECT * FROM TEST_ACCOUNT"

    val queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv)
    val relNode = queryExec.parseAndOptimize(sql)
    mask.doSetRootRelNode(relNode)


    val df = ss.createDataFrame(
      ss.sparkContext.parallelize(Seq(Row(123, 1, 2, "9999", "CN"))),
      StructType(
        List(
          StructField("ACCOUNT_ID", DataTypes.IntegerType),
          StructField("ACCOUNT_BUYER_LEVEL", DataTypes.IntegerType),
          StructField("ACCOUNT_SELLER_LEVEL", DataTypes.IntegerType),
          StructField("ACCOUNT_CONTACT", DataTypes.StringType),
          StructField("ACCOUNT_COUNTRY", DataTypes.StringType)
        ))
    )
    val masked = mask.doMaskResult(df)
    assertResult(Seq(Row(0, 0, null, "****", "****"))) {
      masked.collect()
    }
  }
}
