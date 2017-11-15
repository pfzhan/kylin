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
package org.apache.spark.sql.execution.utils

import java.io.{IOException, ObjectInputStream}
import java.util
import java.util.concurrent.TimeUnit

import com.google.common.cache.{
  Cache,
  CacheBuilder,
  RemovalListener,
  RemovalNotification
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kylin.common.KylinConfig

import scala.collection.JavaConverters._

class CubePathCache {}

object CubePathCache {
  val cubeMappingCache
    : Cache[String, util.Map[Long, util.Set[String]]] = CacheBuilder.newBuilder
    .maximumSize(10000)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .removalListener(
      new RemovalListener[String, util.Map[Long, util.Set[String]]]() {

        override def onRemoval(
            notification: RemovalNotification[String,
                                              util.Map[Long, util.Set[String]]])
          : Unit = {}
      })
    .build
    .asInstanceOf[Cache[String, util.Map[Long, util.Set[String]]]]
    // scalastyle:off
  var TEST_WORK_DIR: String = System.getProperty("user.dir") + "/../../extensions/storage-parquet/src/test/resources/"

  def debugCubeFileName(cubeid: Long): Seq[String] = {
    val cubeInfoPath = new Path(TEST_WORK_DIR + "cube/CUBE_INFO")
    var map: util.Map[Long, util.Set[String]] = null
    val inputStream =
      cubeInfoPath.getFileSystem(new Configuration()).open(cubeInfoPath)
    val obj: ObjectInputStream = new ObjectInputStream(inputStream)
    try {
      map = obj.readObject.asInstanceOf[util.Map[Long, util.Set[String]]]
    } finally if (inputStream != null) inputStream.close()
    val paths = map.get(cubeid)
    paths.asScala
      .map(new Path(_))
      .toSeq
      .map(sandboxPath => TEST_WORK_DIR + "cube/" + sandboxPath.getName)
  }

  def cuboidFileSeq(cuboid: Long,
                    cubeId: String,
                    segmentId: String): Seq[String] = {
    val workingDir = KylinConfig.getInstanceFromEnv.getHdfsWorkingDirectory
    val SCHEMA_HINT = "://"
    val cubeMapping: util.Map[Long, util.Set[String]] =
      readCubeMappingInfo(cubeId.toString, segmentId)
    if (cubeMapping.isEmpty) {
      return Seq()
    }
    val parquetPathCollection: util.Set[String] = cubeMapping.get(cuboid)

    parquetPathCollection.asScala.map { path =>
      if (path.contains(SCHEMA_HINT)) {
        path
      } else { workingDir + "/" + path }
    }.toSeq
  }
  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readCubeMappingInfo(
      cubeId: String,
      segmentId: String): util.Map[Long, util.Set[String]] = {
    val cubeInfoPath = new Path(
      new StringBuilder(KylinConfig.getInstanceFromEnv.getHdfsWorkingDirectory)
        .append("parquet/")
        .append(cubeId)
        .append("/")
        .append(segmentId)
        .append("/")
        .append("CUBE_INFO")
        .toString)
    val ifPresent = cubeMappingCache.getIfPresent(cubeInfoPath)
    if (ifPresent != null) return ifPresent
    val fs = cubeInfoPath.getFileSystem(new Configuration())
    if (fs.exists(cubeInfoPath)) {
      var map: util.Map[Long, util.Set[String]] = null
      val inputStream = new ObjectInputStream(fs.open(cubeInfoPath))
      try {
        map =
          inputStream.readObject.asInstanceOf[util.Map[Long, util.Set[String]]]

        cubeMappingCache.put(cubeInfoPath.toString, map)
      } finally if (inputStream != null) inputStream.close()
      map
    } else throw new RuntimeException("Cannot find CubeMappingInfo")
  }
}
