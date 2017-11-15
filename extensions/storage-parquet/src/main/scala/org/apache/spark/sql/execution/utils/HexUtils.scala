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
package org.apache.spark.sql.execution.utils

import io.netty.buffer.{ByteBuf, Unpooled}

/**
  * Created by imad on 11/04/2017.
  */
object HexUtils {
  def toHexString(bytes: Array[Byte]) = {
    val sb = new StringBuilder
    var i = 0
    while ( {
      i < bytes.length
    }) {
      val hex = Integer.toHexString(0xFF & bytes(i))
      if (hex.length == 1) sb.append('0')
      sb.append(hex)

      {
        i += 1;
        i - 1
      }
    }
    sb.toString
  }


  def toBytes(src: String): Array[Byte] = {
    val res = new Array[Byte](src.length / 2)
    val chs = src.toCharArray
    val b = new Array[Int](2)
    var i = 0
    var c = 0
    while ( {
      i < chs.length
    }) {
      var j = 0
      while ( {
        j < 2
      }) {
        if (chs(i + j) >= '0' && chs(i + j) <= '9') b(j) = chs(i + j) - '0'
        else if (chs(i + j) >= 'A' && chs(i + j) <= 'F') b(j) = chs(i + j) - 'A' + 10
        else if (chs(i + j) >= 'a' && chs(i + j) <= 'f') b(j) = chs(i + j) - 'a' + 10
        j += 1
      }
      b(0) = (b(0) & 0x0f) << 4
      b(1) = b(1) & 0x0f
      res(c) = (b(0) | b(1)).toByte
      i += 2
      c += 1
    }
    res
  }


  def findEndOfLine(buffer: ByteBuf): Int = {
    val n = buffer.writerIndex
    var i = buffer.readerIndex
    while ( {
      i < n
    }) {
      val b = buffer.getByte(i)
      //      if (b == '\n') {
      //        return i;
      //      } else
      if (b == '\r' && i < n - 1 && buffer.getByte(i + 1) == '\n') return i // \r\n
      i += 1
    }
    -1 // Not found.

  }


  def getHttp(byteBuf: ByteBuf): String = {
    val str = StringBuilder.newBuilder
    while (HexUtils.findEndOfLine(byteBuf) != -1) {
      val endOfLine = HexUtils.findEndOfLine(byteBuf)
      var buffer = Unpooled.buffer(endOfLine - byteBuf.readerIndex)
      var charSequence = byteBuf.getBytes(byteBuf.readerIndex, buffer, endOfLine - byteBuf.readerIndex)
      byteBuf.readerIndex(endOfLine + 2)
      str.append(new String(buffer.array())).append("\002")
    }
    str.toString()
  }


  def isHttpRequest(byteBuf: ByteBuf): Boolean = {
    val contains = new String(byteBuf.array()).contains _
    contains("http")
  }
}
