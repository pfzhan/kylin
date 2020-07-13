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

package org.apache.spark.sql.udaf

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.{Input, KryoDataInput, KryoDataOutput, Output}
import org.apache.spark.internal.Logging
import org.roaringbitmap.longlong.Roaring64NavigableMap


class BitmapSerAndDeSer extends Logging {
  private var array: Array[Byte] = _
  private var output: Output = _

  def serialize(buffer: Roaring64NavigableMap): Array[Byte] = {
    try {
      if (array == null) {
        array = new Array[Byte](1024 * 1024)
        output = new Output(array)
      }
      buffer.runOptimize()
      output.clear()
      val dos = new KryoDataOutput(output)
      buffer.serialize(dos)
      val i = output.position()
      output.close()
      array.slice(0, i)
    } catch {
      case th: KryoException if th.getMessage.contains("Buffer overflow") =>
        logInfo(s"Resize buffer size to ${array.length * 2}")
        array = new Array[Byte](array.length * 2)
        output.setBuffer(array)
        serialize(buffer)
      case th =>
        throw th
    }
  }

  def deserialize(bytes: Array[Byte]): Roaring64NavigableMap = {
    val bitMap = new Roaring64NavigableMap()
    if (bytes.nonEmpty) {
      bitMap.deserialize(new KryoDataInput(new Input(bytes)))
    }
    bitMap
  }
}

object BitmapSerAndDeSer {
  private val serAndDeSer: ThreadLocal[BitmapSerAndDeSer] = new ThreadLocal[BitmapSerAndDeSer]() {
    override def initialValue(): BitmapSerAndDeSer = new BitmapSerAndDeSer
  }

  def get(): BitmapSerAndDeSer = serAndDeSer.get()
}
