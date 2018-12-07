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

package org.apache.spark.sql.execution.datasource

import io.kyligence.kap.cube.model.{NCuboidLayout, NDataflow}
import org.apache.kylin.gridtable.GTInfo
import org.apache.kylin.metadata.model.FunctionDesc
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparderEnv, SparkSession}

import scala.collection.JavaConverters._

// scalastyle:off
case class CubeRelation(tableName: String
                        , dataflow: NDataflow
                        , info: GTInfo
                        , cuboid: NCuboidLayout
                        , metrics: java.util.Set[FunctionDesc]
                       )(val sparkSession: SparkSession) extends KylinRelation {

  def initSchema(): StructType = {
    val columnMapping = initColumnNameMapping(cuboid)
    val sourceSchema = SchemaProcessor.buildGTSchema(columnMapping.map(_._1), info, tableName)
    sourceSchema
  }

  def initColumnNameMapping(cuboid: NCuboidLayout): Array[(String, String)] = {
    val cols = cuboid.getColumns.asScala.map(col =>
      (col.getIdentity.replace(".", "_"), col.getType.getName)
    ).toArray

    // "getOrderedMeasures" returns a map, need toList
    val measures = cuboid.getOrderedMeasures.asScala.toList.map(measure =>
      (measure._2.getName.replace(".", "_"), measure._2.getFunction.getReturnType)
    ).toArray

    cols ++ measures
  }

  override def sqlContext: SQLContext = SparderEnv.getSparkSession.sqlContext

  override def schema: StructType = initSchema()

}
