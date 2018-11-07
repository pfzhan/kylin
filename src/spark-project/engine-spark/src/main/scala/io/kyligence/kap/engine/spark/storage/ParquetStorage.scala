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

package io.kyligence.kap.engine.spark.storage

import io.kyligence.kap.cube.model.NDataCuboid
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class ParquetStorage
    extends NSparkCubingEngine.NSparkCubingStorage
    with Serializable {
  override def getCuboidData(cuboid: NDataCuboid,
                             ss: SparkSession): DataFrame = {
    val path = NSparkCubingUtil.getStoragePath(cuboid)
    ss.read.parquet(path)
  }

  override def saveCuboidData(cuboid: NDataCuboid,
                              data: DataFrame,
                              ss: SparkSession): Unit = {
    val path = NSparkCubingUtil.getStoragePath(cuboid)
    data.write
      .mode(SaveMode.Overwrite)
      .option("parquet.block.size", 10 * 1024 * 1024)
      .parquet(path)

  }

}
