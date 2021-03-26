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

import java.nio.{BufferOverflowException, ByteBuffer}
import org.apache.spark.internal.Logging
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


class BitmapSerAndDeSer extends Logging {

  def serialize(buffer: Roaring64NavigableMap): Array[Byte] = {
    buffer.runOptimize()
    serialize(buffer, 1024 * 1024)
  }

  @tailrec
  final def serialize(buffer: Roaring64NavigableMap, capacity: Int): Array[Byte] = {
    Try {
      val byteBuffer = ByteBuffer.allocate(capacity)
      buffer.serialize(byteBuffer)
      byteBuffer.flip()
      if( byteBuffer.limit() == byteBuffer.capacity()) {
        byteBuffer.array()
      } else {
        val result = new Array[Byte](byteBuffer.limit())
        byteBuffer.get(result, 0, byteBuffer.limit())
        result
      }
    } match {
      case Success(value) => value
      case Failure(e: BufferOverflowException ) =>
        logInfo(s"Resize buffer size to ${capacity * 2}")
        serialize(buffer, capacity * 2)
      case Failure(e) => throw e
    }
  }

  def deserialize(bytes: Array[Byte]): Roaring64NavigableMap = {
    val bitMap = new Roaring64NavigableMap()
    if (bytes.nonEmpty) {
      bitMap.deserialize(ByteBuffer.wrap(bytes))
    }
    bitMap
  }
}

// TODO: remove BitmapSerAndDeSerObj and use BitmapSerAndDeSer eventually
object BitmapSerAndDeSerObj extends  BitmapSerAndDeSer

object BitmapSerAndDeSer {
  private val serAndDeSer: ThreadLocal[BitmapSerAndDeSer] = new ThreadLocal[BitmapSerAndDeSer]() {
    override def initialValue(): BitmapSerAndDeSer = new BitmapSerAndDeSer
  }

  def get(): BitmapSerAndDeSer = serAndDeSer.get()
}
