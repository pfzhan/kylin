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
package org.apache.spark.sql.common

import java.io.File

import io.kyligence.kap.common.util.{NLocalFileMetadataTestCase, Unsafe}
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}


trait LocalMetadata extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>
  val metadata = "../examples/test_case_data/localmeta"
  var metaStore: NLocalFileMetadataTestCase = _
  var zkTestServer: TestingServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore = new NLocalFileMetadataTestCase
    if (new File(metadata).exists()) {
      metaStore.createTestMetadata(metadata)
    } else {
      metaStore.createTestMetadata("../" + metadata)
    }
    zkTestServer = new TestingServer(true)
    Unsafe.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString)
  }

  protected def overwriteSystemProp(key: String, value: String): Unit = {
    metaStore.overwriteSystemProp(key, value)
  }

  override protected def afterEach(): scala.Unit = {
    metaStore.restoreSystemProps()
  }


  override def afterAll() {
    super.afterAll()
    try {
      metaStore.restoreSystemProps()
      metaStore.cleanupTestMetadata()
      if (zkTestServer != null) {
        zkTestServer.close()
        Unsafe.clearProperty("kylin.env.zookeeper-connect-string")
      }
    } catch {
      case ignore: Exception =>
    }
  }
}
