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
package io.kyligence.kap.query.runtime.plan

import io.kyligence.kap.query.relnode.KapValuesRel
import org.apache.spark.sql.{DataFrame, Row, SparkOperation}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.JavaConverters._

object ValuesPlan {
  def values(rel: KapValuesRel): DataFrame = {

    val schema = StructType(rel.getRowType.getFieldList.asScala.map { field =>
      StructField(
        field.getName,
        SparderTypeUtil.convertSqlTypeToSparkType(field.getType))
    })
    val rows = rel.tuples.asScala.map { tp =>
      Row.fromSeq(tp.asScala.map(lit => SparderTypeUtil.getValueFromRexLit(lit)))
    }.asJava
    if (rel.tuples.size() == 0) {
      SparkOperation.createEmptyDataFrame(schema)
    } else {
      SparkOperation.createConstantDataFrame(rows, schema)
    }
  }
}
