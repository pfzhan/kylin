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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.mutable

// scalastyle:off
@SerialVersionUID(1)
case class OptIntersectCount(child: Expression,
                             arrayExpr: Expression,
                             mutableAggBufferOffset: Int = 0,
                             inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Counter] with Serializable with Logging {

  override def dataType: DataType = MapType(StringType, BinaryType)

  override def children: Seq[Expression] = child :: arrayExpr :: Nil

  override def nullable: Boolean = false

  override def createAggregationBuffer(): Counter = new Counter()

  override def update(buffer: Counter, input: InternalRow): Counter = {
    val colValue = child.eval(input)
    val arrayData = arrayExpr.eval(input).asInstanceOf[ArrayData]
    val tags = (0 until arrayData.numElements).map { i =>
      arrayData.getUTF8String(i)
    }.toArray

    if (colValue != null && tags != null && tags.nonEmpty) {
      buffer.add(tags, colValue.asInstanceOf[Long])
    }
    buffer
  }

  override def merge(buffer: Counter, input: Counter): Counter = {
    if (input != null) {
      buffer.merge(input)
    }
    buffer
  }

  override def eval(buffer: Counter): Any = {
    val map = buffer.map.map{ e =>
      e._1 -> serializeBitmap(e._2)
    }.toMap
    ArrayBasedMapData.apply(map)
  }

  var array: Array[Byte] = _
  var output: Output = _

  override def serialize(counter: Counter): Array[Byte] = {
    try {
      if (counter != null) {
        if (array == null) {
          array = new Array[Byte](1024 * 1024)
          output = new Output(array)
        }
        output.clear()
        val kryo = new KryoDataOutput(output)
        val map = counter.map
        kryo.writeInt(map.size)
        map.foreach { entry =>
          kryo.writeUTF(entry._1.toString)
          entry._2.serialize(kryo)
        }
        val i = output.position()
        output.close()
        array.slice(0, i)
      } else {
        Array.empty[Byte]
      }
    } catch {
      case th: KryoException if th.getMessage.contains("Buffer overflow") =>
        logInfo(s"Resize buffer size to ${array.length * 2}")
        array = new Array[Byte](array.length * 2)
        output.setBuffer(array)
        serialize(counter)
      case th =>
        throw th
    }
  }

  def serializeBitmap(buffer: Roaring64NavigableMap): Array[Byte] = {
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
        serializeBitmap(buffer)
      case th =>
        throw th
    }
  }

  override def deserialize(bytes: Array[Byte]): Counter = {
    val counter = new Counter
    if (bytes.nonEmpty) {
      val input = new KryoDataInput(new Input(bytes))
      val size = input.readInt()
      if (size != 0) {
        val map = counter.map
        for (_: Int <- 1 to size) {
          val key = input.readUTF()
          val bitmap = new Roaring64NavigableMap()
          bitmap.deserialize(input)
          map.put(UTF8String.fromString(key), bitmap)
        }
      }
    }
    counter
  }

  override val prettyName: String = "opt_intersect_count"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }
}

class Counter() {
  val map: mutable.Map[UTF8String, Roaring64NavigableMap] = new mutable.HashMap()

  def merge(other: Counter): Unit = {
    val otherMap = other.map
    otherMap.foreach { entry =>
      val key = entry._1
      val value = entry._2
      if (map.contains(key)) {
        map.apply(key).or(value)
      } else {
        map.put(key, value)
      }
    }
  }

  def add(keys: Array[UTF8String], value: Long): Unit = {
    keys.foreach { key =>
      if (map.contains(key)) {
        map.apply(key).add(value)
      } else {
        val newBitmap = new Roaring64NavigableMap()
        newBitmap.add(value)
        map.put(key, newBitmap)
      }
    }
  }
}