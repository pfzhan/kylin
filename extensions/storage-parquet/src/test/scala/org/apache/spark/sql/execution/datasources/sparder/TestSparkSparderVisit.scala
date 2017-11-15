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

import java.io.ObjectInputStream
import java.nio.ByteBuffer
import java.util

import io.kyligence.kap.storage.parquet.cube.spark.rpc.RDDPartitionResult
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.gridtable.GTScanRequest
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.TestConstants

import scala.collection.JavaConverters._

class TestSparkSparderVisit(request: SparkJobProtos.SparkJobRequestPayload,
                            sc: JavaSparkContext,
                            streamIdentifier: String) extends SparkSparderVisit(request, sc, streamIdentifier) {
  override lazy  val scanRequest: GTScanRequest = TestGtscanRequest.serializer.deserialize(ByteBuffer.wrap(request.getGtScanRequest.toByteArray))

  override def cuboidFileSeq(): Seq[String] = {
    getCubeFileName(cuboid).map(sandboxPath => TestConstants.TEST_WORK_DIR + "cube/" + sandboxPath.getName)
  }

  var task: (TestSparkSparderVisit) => org.apache.kylin.common.util.Pair[util.Iterator[RDDPartitionResult], JavaRDD[RDDPartitionResult]] = _


  override def executeTask(): org.apache.kylin.common.util.Pair[util.Iterator[RDDPartitionResult], JavaRDD[RDDPartitionResult]] = {
    task(this)
    //      .rdd.mapPartitions{
    //      iterator =>{
    //        transform(iterator)
    //      }
    //    })
  }

  def getCubeFileName(cubeid: Long): Seq[Path] = {
    val cubeInfoPath = new Path(TestConstants.TEST_WORK_DIR + "cube/CUBE_INFO")
    var map: util.Map[Long, util.Set[String]] = null
    val inputStream = cubeInfoPath.getFileSystem(new Configuration()).open(cubeInfoPath)
    try {
      val obj: ObjectInputStream = new ObjectInputStream(inputStream)
      try {
        map = obj.readObject.asInstanceOf[util.Map[Long, util.Set[String]]]
      } finally if (inputStream != null) inputStream.close()
    }
    val paths = map.get(cubeid)
    paths.asScala.map(new Path(_)).toSeq

  }

}

object TestSparkSparderVisit {
  def apply(request: SparkJobProtos.SparkJobRequestPayload, sc: JavaSparkContext,
            streamIdentifier: String): TestSparkSparderVisit = new TestSparkSparderVisit(request, sc, streamIdentifier)
}
