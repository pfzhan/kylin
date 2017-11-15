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

import java.nio.ByteBuffer

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.kylin.gridtable.GTScanRequest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, DecimalType, LongType, StructType}


class RowSpliter(structType: StructType, gTScanRequest: GTScanRequest) {
  //todo check null
  private val columns = gTScanRequest.getColumns.iterator()
  private val map = structType.map(field => {
    val colId = columns.next()
    (field, colId)
  })
  val allocate: ByteBuffer = ByteBuffer.allocate(gTScanRequest.getInfo.getMaxLength)

  def split(row: Row, byteArrayOutputStream: ByteArrayOutputStream): Unit = {
    var colCount = 0
    for ((field, colId) <- map) {
      field.dataType match {
        case BinaryType =>
          val bytes = row.apply(colCount).asInstanceOf[Array[Byte]]
          if(colCount ==3){
            println("length : "+bytes.length)
          }
          allocate.put(bytes)
        case LongType =>
          gTScanRequest.getInfo.getCodeSystem.encodeColumnValue(colId, row.getLong(colCount), allocate)
        case key: DecimalType =>
          val start = allocate.position()
          gTScanRequest.getInfo.getCodeSystem.encodeColumnValue(colId,row.apply(colCount), allocate)
          println("length : "+( allocate.position() - start))
      }
      colCount = colCount + 1
    }
    byteArrayOutputStream.write(allocate.array(), 0, allocate.position())
    allocate.clear()
  }
}
