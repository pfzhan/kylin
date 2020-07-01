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

package io.kyligence.kap.query.util

import io.kyligence.kap.common.util.AddressUtil
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.slf4j.LoggerFactory

object SparderAppUtil {

  private val log = LoggerFactory.getLogger(SparderAppUtil.getClass)

  val ROLL_LOG_DIR_NAME_PREFIX = "eventlog_v2_"

  def getValidSparderApps(startTime: Long, endTime: Long): List[FileStatus] = {
    val logDir = getSparderEvenLogDir
    HadoopUtil.getFileSystem(logDir).listStatus(new Path(logDir)).toList
      .filter(fileStatus => {
        var valid = false
        try {
          val fileInfo = fileStatus.getPath.getName.split("#")
          valid = fileInfo.length == 2 && fileInfo(1).toLong <= endTime && fileStatus.getModificationTime >= startTime
        } catch {
          case e: Exception =>
            log.error("Check sparder appId time range failed.", e)
        }
        valid
      })
  }

  def getSparderEvenLogDir(): String = {
    KapConfig.wrap(KylinConfig.getInstanceFromEnv).getSparkConf.get("spark.eventLog.dir") + "/" + AddressUtil.getLocalServerInfo
  }
}
