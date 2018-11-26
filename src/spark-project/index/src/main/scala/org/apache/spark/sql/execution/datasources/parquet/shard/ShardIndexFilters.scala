/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package org.apache.spark.sql.execution.datasources.parquet.shard

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

private[shard] case class ShardIndexFilters(files: Seq[FileStatus], indexSchema: StructType) {

  private def selectFiles(attribute: String, values: Array[Any]): Set[FileStatus] = {
    require(indexSchema.fields.head.name == attribute)
    values.map { value =>
      val hash = Murmur3HashFunction.hash(value, indexSchema.fields.head.dataType, 42)
      val i = if (hash >= 0) {
        hash % files.length
      } else {
        files.length + hash % files.length
      }
      files.apply(i.toInt)
    }.toSet
  }

  def filterFiles(filter: Filter): Set[FileStatus] = {
    filter match {
      case EqualTo(attribute: String, value: Any) =>
        selectFiles(attribute, Array(value))
      case In(attribute: String, values: Array[Any]) =>
        selectFiles(attribute, values)
      case And(left: Filter, right: Filter) =>
        filterFiles(left) intersect filterFiles(right)
      case Or(left: Filter, right: Filter) =>
        filterFiles(left) union filterFiles(right)
      case Not(child: Filter) =>
        files.toSet diff filterFiles(child)
      case _ => files.toSet
    }
  }

  def pruningFilter(filter: Filter): Filter = {
    filter match {
      case And(left, right) =>
        (pruningFilter(left), pruningFilter(right)) match {
          case (Invalid(), Invalid()) => Invalid()
          case (Invalid(), _right) => _right
          case (_left, Invalid()) => _left
          case (_left, _right) => And(_left, _right)
        }
      case Or(left, right) =>
        (pruningFilter(left), pruningFilter(right)) match {
          case (Invalid(), Invalid()) => Invalid()
          case (Invalid(), _) => Invalid()
          case (_, Invalid()) => Invalid()
          case (_left, _right) => Or(_left, _right)
        }
      case Not(child) =>
        pruningFilter(child) match {
          case Invalid() =>
            Invalid()
          case other => Not(other)
        }
      case equalTo: EqualTo =>
        equalTo
      case in: In =>
        in
      case _ =>
        Invalid()
    }
  }
}
