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

package io.kyligence.kap.engine.spark.builder

import java.util.Objects

import com.google.common.collect.Maps
import io.kyligence.kap.common.util.Unsafe
import javax.security.auth.login.AppConfigurationEntry
import org.apache.hadoop.security.authentication.util.KerberosUtil
import org.apache.zookeeper.client.ZooKeeperSaslClient

class ZKJaasConfiguration(private val principal: String,
                          private val keyTab: String) extends javax.security.auth.login.Configuration {

  private val TRUE: String = "true"
  private val FALSE: String = "false"
  private val DEFAULT_CONTEXT_NAME = "Client"

  Unsafe.setProperty("zookeeper.sasl.client", TRUE)

  private val contextName = System.getProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, DEFAULT_CONTEXT_NAME)

  private def getSpecifiedOptions: java.util.Map[String, String] = {
    val options = Maps.newHashMap[String, String]()
    val vendor = System.getProperty("java.vendor")
    val isIBM = if (Objects.isNull(vendor)) {
      false
    } else {
      vendor.contains("IBM")
    }
    if (isIBM) {
      options.put("credsType", "both")
    } else {
      options.put("useKeyTab", TRUE)
      options.put("useTicketCache", FALSE)
      options.put("doNotPrompt", TRUE)
      options.put("storeKey", TRUE)
    }

    if (Objects.nonNull(keyTab)) {
      if (isIBM) {
        options.put("useKeytab", keyTab)
      } else {
        options.put("keyTab", keyTab)
        options.put("useKeyTab", TRUE)
        options.put("useTicketCache", FALSE)
      }
    }
    options.put("principal", principal)
    options
  }

  private def getSpecifiedEntry: Array[AppConfigurationEntry] = {
    val array = new Array[AppConfigurationEntry](1)
    array(0) = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName, //
      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, getSpecifiedOptions)
    array
  }

  override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
    if (name.equals(contextName)) {
      return getSpecifiedEntry
    }
    try {
      val conf = javax.security.auth.login.Configuration.getConfiguration
      conf.getAppConfigurationEntry(name)
    } catch {
      case _: SecurityException => null
    }
  }
}
