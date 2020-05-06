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

import java.io.{DataInput, DataOutput}
import java.nio.charset.StandardCharsets

import org.apache.spark.unsafe.types.UTF8String

@SerialVersionUID(1)
sealed trait NullSafeValueSerializer {
  final def serialize(output: DataOutput, value: Any): Unit = {
    if (value == null) {
      output.writeInt(0)
    } else {
      serialize0(output, value.asInstanceOf[Any])
    }
  }

  @inline protected def serialize0(output: DataOutput, value: Any): Unit

  def deserialize(input: DataInput): Any = {
    val length = input.readInt()
    if (length == 0) {
      null
    } else {
      deSerialize0(input, length)
    }
  }

  @inline protected def deSerialize0(input: DataInput, length: Int): Any
}

@SerialVersionUID(1)
class BooleanSerializer extends NullSafeValueSerializer {
  override protected def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(1)
    output.writeBoolean(value.asInstanceOf[Boolean])
  }

  override protected def deSerialize0(input: DataInput, length: Int): Any = {
    input.readBoolean()
  }
}

@SerialVersionUID(1)
class ByteSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(1)
    output.writeByte(value.asInstanceOf[Byte])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readByte()
  }
}

@SerialVersionUID(1)
class ShortSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(2)
    output.writeShort(value.asInstanceOf[Short])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readShort()
  }
}

@SerialVersionUID(1)
class IntegerSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(4)
    output.writeInt(value.asInstanceOf[Int])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readInt()
  }
}

@SerialVersionUID(1)
class FloatSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(4)
    output.writeFloat(value.asInstanceOf[Float])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readFloat()
  }
}

@SerialVersionUID(1)
class DoubleSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(8)
    output.writeDouble(value.asInstanceOf[Double])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readDouble()
  }
}

@SerialVersionUID(1)
class LongSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(8)
    output.writeLong(value.asInstanceOf[Long])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readLong()
  }
}

@SerialVersionUID(1)
class StringSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    val bytes = value.toString.getBytes
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    val bytes = new Array[Byte](length)
    input.readFully(bytes)
    UTF8String.fromBytes(bytes)
  }
}

@SerialVersionUID(1)
class DecimalSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    val decimal = value.asInstanceOf[BigDecimal]
    val bytes = decimal.toString().getBytes(StandardCharsets.UTF_8)
    output.writeInt(1 + bytes.length)
    output.writeByte(decimal.scale)
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    val scale = input.readByte()
    val length = input.readInt()
    val bytes = new Array[Byte](length)
    input.readFully(bytes)
    val decimal = BigDecimal.apply(new String(bytes, StandardCharsets.UTF_8))
    decimal.setScale(scale)
  }
}

