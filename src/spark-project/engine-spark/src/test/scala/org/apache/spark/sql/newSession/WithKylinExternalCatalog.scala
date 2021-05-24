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
package org.apache.spark.sql.newSession

import java.io.File

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase
import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.kylin.external.KylinSharedState
import org.apache.spark.sql.{SparderEnv, SparkSession}
import org.scalatest.BeforeAndAfterAll

trait WithKylinExternalCatalog extends SparkFunSuite with BeforeAndAfterAll {
  protected val ut_meta = "../examples/test_case_data/localmeta"
  protected val additional = "../../build/conf/spark-executor-log4j.xml"
  protected def metadata : Seq[String] = {
      Seq(fitPathForUT(ut_meta))
  }
  private def fitPathForUT(path: String) = {
    if (new File(path).exists()) {
      path
    } else {
      s"../$path"
    }
  }

  protected def clearSparkSession(): Unit = {
    if (SparderEnv.isSparkAvailable) {
      SparderEnv.getSparkSession.stop()
    }
    SparkSession.setActiveSession(null)
    SparkSession.setDefaultSession(null)
  }

  protected lazy val spark: SparkSession = SparderEnv.getSparkSession
  lazy val kylinConf: KylinConfig = KylinConfig.getInstanceFromEnv
  lazy val metaStore: NLocalFileMetadataTestCase = new NLocalFileMetadataTestCase

  protected def overwriteSystemProp (key: String, value: String): Unit = {
    metaStore.overwriteSystemProp(key, value)
  }

  override def beforeAll(): Unit = {
    metaStore.createTestMetadata(metadata: _*)
    metaStore.overwriteSystemProp("kylin.use.external.catalog", "io.kyligence.kap.engine.spark.mockup.external.FileCatalog")
    metaStore.overwriteSystemProp("kylin.NSparkDataSource.data.dir", s"${kylinConf.getMetadataUrlPrefix}/../data")
    metaStore.overwriteSystemProp("kylin.source.provider.9", "io.kyligence.kap.engine.spark.source.NSparkDataSource")
    metaStore.overwriteSystemProp("kylin.query.engine.sparder-additional-files", fitPathForUT(additional))
    metaStore.overwriteSystemProp("kylin.source.jdbc.adaptor", "Set By WithKylinExternalCatalog")
    metaStore.overwriteSystemProp("kylin.source.jdbc.driver", "Set By WithKylinExternalCatalog")
    metaStore.overwriteSystemProp("kylin.source.jdbc.connection-url", "Set By WithKylinExternalCatalog")


    assert(kylinConf.isDevOrUT)
    assert(spark.sharedState.isInstanceOf[KylinSharedState])
    assert(spark.sessionState.catalog.databaseExists("FILECATALOGUT"))
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    clearSparkSession()
    metaStore.restoreSystemProps()
    metaStore.cleanupTestMetadata()
  }
}
