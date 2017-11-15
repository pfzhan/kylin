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
package org.apache.spark.sql

import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.cube.CubeManager
import org.apache.kylin.dict.DictionaryInfoSerializer
import org.apache.kylin.metadata.TableMetadataManager
import org.apache.kylin.metadata.project.ProjectManager
import org.apache.spark.sql.execution.datasources.sparder.SparderServiceFunSuite
import org.apache.spark.sql.manager.SparderDictionaryManager
import sun.misc.Unsafe

class SparderDictionaryManagerTest extends SparderServiceFunSuite {
  sparkNeed = false
  val dictPath = new Path(
    "/kylin/sparder/dict/DEFAULT.TEST_KYLIN_FACT/LEAF_CATEG_ID/38309b83-29b4-4b1c-8b21-ecf4f8c2ee81.dict")
  val hbaseDictPath =
    "/dict/DEFAULT.TEST_KYLIN_FACT/LEAF_CATEG_ID/f0e2f1f4-8b0e-4dd4-a1a9-a63768548f68.dict"
  test("sdad") {
    val env = KylinConfig.getInstanceFromEnv
    val instances = ProjectManager.getInstance(env).listAllProjects()
    val cube = CubeManager.getInstance(env).listAllCubes()
    val segment = cube.get(4).getSegments.getFirstSegment
    val desc = segment.getCubeDesc
    val desc1 = desc.getRowkey.getRowKeyColumns.apply(3)
    if (desc1.isUsingDictionary) {
      val dictPath = segment.getDictResPath(desc1.getColRef)
      val store = TableMetadataManager
        .getInstance(KylinConfig.getInstanceFromEnv)
        .getStore
      val conf = new Configuration
      val fileSystem = FileSystem.get(conf)
      val newPath =
        Path.mergePaths(new Path("/kylin/sparder"), new Path(dictPath))
      val stream = fileSystem.create(newPath)
      val raw = store.getResource(dictPath)
      IOUtils.copyBytes(raw.inputStream, stream, 4096)
      print(dictPath)
    }
    println(env)
  }

  test("scp to ") {
    val conf = new Configuration
    val fileSystem = FileSystem.get(conf)
    val stream = fileSystem.open(dictPath)
    val serializer = new DictionaryInfoSerializer()
    val info1 = serializer.deserialize(stream)
    println(info1)
  }

  test("add") {
    val str = SparderDictionaryManager.add(hbaseDictPath)
    assert(str.equals(
      "hdfs://sandbox.hortonworks.com:8020/kylin/kylin_default_instance3/sparder/dict/DEFAULT.TEST_KYLIN_FACT/LEAF_CATEG_ID/38309b83-29b4-4b1c-8b21-ecf4f8c2ee81.dict"))
  }

  test("test dictionary") {
    val serializer = new DictionaryInfoSerializer()
    //    val dictionary = new Path("file:///Users/imad/f0e2f1f4-8b0e-4dd4-a1a9-a63768548f68.dict")
    val dictionary =
      new Path("file:///Users/imad/bb038bfd-fe5d-4a91-b5c8-dfd5730d3986.dict")
    val fileSystem = dictionary.getFileSystem(new Configuration())
    val l2 = Runtime.getRuntime.totalMemory()
    val stream = fileSystem.open(dictionary)
    val dictionaryInfo = serializer.deserialize(stream)
    val dictionaryObject = dictionaryInfo.getDictionaryObject
    val id = dictionaryObject.getMaxId
    var start = System.currentTimeMillis()
    var l = System.nanoTime()
    val map = new Array[Array[Byte]](id)
    for (i <- Range(0, id)) {
      map(i) = dictionaryObject.getValueByteFromId(i)
      //      println(str)
    }
    var count = 20000000
    println((System.nanoTime() - l) / id)
    println(System.currentTimeMillis() - start)
    println(id)
    println("start ......")
    val dst = new Array[Byte](52)
    val unsafe = {
      val f = classOf[Unsafe].getDeclaredField("theUnsafe")
      f.setAccessible(true)
      f.get(null).asInstanceOf[Unsafe]
    }

    val random = new Random(id)
    start = System.currentTimeMillis()
    l = System.nanoTime()
    var max = 0
    var totalCopyTime = 0L
    for (i <- Range(0, count)) {
      val src = map.apply(random.nextInt(id))
      //      System.arraycopy(src, 0, dst, 0, src.length)
      unsafe.copyMemory(src,
                        Unsafe.ARRAY_BYTE_BASE_OFFSET + 0,
                        null,
                        count,
                        src.length)
    }
    println((System.nanoTime() - l) / count)
    println(" copy " + totalCopyTime / count)
    println(System.currentTimeMillis() - start)
    println(id)
    println(max)
  }
}
