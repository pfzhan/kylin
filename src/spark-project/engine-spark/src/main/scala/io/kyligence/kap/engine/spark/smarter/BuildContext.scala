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
package io.kyligence.kap.engine.spark.smarter

import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.util.control.NonFatal

class BuildContext(sparkContext: SparkContext, kylinConfig: KylinConfig) extends Logging {

  /* ------------------------------------------------------------------------------------- *
 | Private variables. These variables keep the internal state of the context, and are    |
 | not accessible by the outside world.                                                  |
 * ------------------------------------------------------------------------------------- */

  private var _appStatusTracker: BuildAppStatusTracker = _
  private var _appStatusStore: BuildAppStatusStore = _

  /* ------------------------------------------------------------------------------------- *
 | Accessors and public fields. These provide access to the internal state of the        |
 | context.                                                                              |
 * ------------------------------------------------------------------------------------- */

  def appStatusTracker: BuildAppStatusTracker = _appStatusTracker

  def appStatusStore: BuildAppStatusStore = _appStatusStore

  try {
    _appStatusStore = new BuildAppStatusStore(kylinConfig, sparkContext)
    _appStatusTracker = new BuildAppStatusTracker(kylinConfig, sparkContext, _appStatusStore)
  } catch {
    case NonFatal(e) =>
      logError("Error initializing BuildContext.", e)
  }

  def stop(): Unit = {
    if (_appStatusTracker != null) {
      Utils.tryLogNonFatalError {
        _appStatusTracker.shutdown()
      }
    }
    logInfo("Stop BuildContext succeed.")
  }
}
