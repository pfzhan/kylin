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

package io.kyligence.kap.query.asyncprofiler

/** simple command in string
 *  messages are in form of {command}{executor-id}:{param}
 *  commands are limited to fixed 3 characters
 */
object Message {
  /** issue from driver */
  // profiling command
  val START = "STA"
  val DUMP = "DMP"
  val STOP = "STP"
  val NOP = "NOP"

  /** issue from executors */
  // ask for next command
  val NEXT_COMMAND = "NEX"
  // send back profiling result
  val RESULT = "RES"

  private val COMMAND_LEN = 3
  private val SEPARATOR = ":"

  def getCommand(msg: String): String = {
    msg.substring(0, COMMAND_LEN)
  }

  def getId(msg: String): String = {
    msg.substring(3, msg.indexOf(SEPARATOR))
  }

  def getParam(msg: String): String = {
    msg.substring(msg.indexOf(SEPARATOR) + 1)
  }

  def createDriverMessage(cmd: String, param: String = ""): String = {
    s"$cmd-1$SEPARATOR$param"
  }

  def createExecutorMessage(cmd: String, id: String, param: String = ""): String = {
    s"$cmd$id$SEPARATOR$param"
  }

  /**
    *
    * @param msg
    * @return (cmd, executor-id, param)
    */
  def processMessage(msg: String): (String, String, String) = {
    (getCommand(msg), getId(msg), getParam(msg))
  }
}

