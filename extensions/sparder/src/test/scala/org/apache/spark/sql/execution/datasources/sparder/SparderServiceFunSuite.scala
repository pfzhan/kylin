
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.spark.sql.execution.datasources.sparder

import java.io.File

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.ClassUtil

class SparderServiceFunSuite extends SparderFunSuite {
  val SERVICES_WITH_CACHE = Array("org.apache.kylin.cube.CubeManager", "org.apache.kylin.cube.CubeDescManager", "org.apache.kylin.dict.lookup.SnapshotManager", "org.apache.kylin.dict.DictionaryManager", "org.apache.kylin.storage.hybrid.HybridManager", "org.apache.kylin.metadata.realization.RealizationRegistry", "org.apache.kylin.metadata.project.ProjectManager", "org.apache.kylin.metadata.MetadataManager", "org.apache.kylin.metadata.cachesync.Broadcaster", "org.apache.kylin.metadata.badquery.BadQueryHistoryManager", "org.apache.kylin.job.impl.threadpool.DistributedScheduler", "org.apache.kylin.job.execution.ExecutableManager", "org.apache.kylin.job.dao.ExecutableDao")

  var SANDBOX_TEST_DATA = "../examples/test_case_data/sandbox"

  override def beforeInit(): Unit = {
    staticCreateTestMetadata()
  }

  ClassUtil.addClasspath(new File(SANDBOX_TEST_DATA).getAbsolutePath());

  @throws[Exception]
  def staticCreateTestMetadata(): Unit = {
    staticCreateTestMetadata(SANDBOX_TEST_DATA)
  }

  def staticCreateTestMetadata(kylinConfigFolder: String): Unit = {
    KylinConfig.destroyInstance()
    if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null) System.setProperty(KylinConfig.KYLIN_CONF, kylinConfigFolder)
  }

  @throws[Exception]
  def createTestMetadata(): Unit = {
    staticCreateTestMetadata()
  }

  def getTestConfig: KylinConfig = KylinConfig.getInstanceFromEnv

  def cleanupTestMetadata(): Unit = {
    staticCleanupTestMetadata()
  }

  override protected def afterAll(): Unit = {
    staticCleanupTestMetadata()
  }

  def staticCleanupTestMetadata(): Unit = {
    cleanupCache()
    System.clearProperty(KylinConfig.KYLIN_CONF)
    KylinConfig.destroyInstance()
  }

  private def cleanupCache(): Unit = {
    for (serviceClass <- SERVICES_WITH_CACHE) {
      try {
        val cls = Class.forName(serviceClass)
        val method = cls.getDeclaredMethod("clearCache")
        method.invoke(null)
      } catch {
        case e: ClassNotFoundException =>

        // acceptable because lower module test does have CubeManager etc on classpath
        case e: Exception =>
          System.err.println("Error clean up cache " + serviceClass)
          e.printStackTrace()
      }
    }
  }
}

