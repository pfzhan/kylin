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
package org.apache.spark.sql.types

import java.nio.ByteBuffer

import org.apache.kylin.measure.BufferedMeasureCodec
import org.apache.kylin.measure.hllc.HLLCounter

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}

case class HLLCUDT(p: Int) extends UserDefinedType[HLLCounter] {
  var byteBuff: ByteBuffer = _

  def this() = this(10)

  override def sqlType: HLLCUDT = HLLCUDT(p)

  override def serialize(obj: HLLCounter): ArrayData = {
    if (byteBuff == null) {
      byteBuff = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE)
    }
    byteBuff.clear()
    obj.writeRegisters(byteBuff)
    new GenericArrayData(byteBuff.array().slice(0, byteBuff.position()))
  }

  override def deserialize(datum: Any): HLLCounter = {
    datum match {
      case data: ArrayData =>
        val hLLc = new HLLCounter(p)
        hLLc.readRegisters(ByteBuffer.wrap(data.toByteArray()))
        hLLc
    }
  }

  override def userClass: Class[HLLCounter] = classOf[HLLCounter]

  private[spark] override def asNullable: HLLCUDT = this

}
