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

package org.apache.spark.sql

import java.io.File
import java.util.Properties

import org.apache.kylin.common.util.ClassUtil
import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession.Builder


object KylinUserSession extends Logging {
  
  implicit class KylinBuilder(builder: Builder) {


    def getOrCreateKylinUserSession(): KylinSession = synchronized {
      logInfo("start new kylin user session ")
      val options =
        getValue("options", builder)
          .asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      setEnvs(options)
      validateEnvs()
      initKylinConfig(options)
      builder.config("user.kylin.session", "true")
      builder.config("spark.hadoop.yarn.timeline-service.enabled", "false")
      KylinSession.KylinBuilder(builder).getOrCreateKylinSession().asInstanceOf[KylinSession]

    }
  }

  def initKylinConfig(options: scala.collection.mutable.HashMap[String, String]): Unit = {
    val metaDataUrl = options.get("kylin.metadata.url")
    assert(metaDataUrl.isDefined, "kylin.metadata.url need to be defined")
    val props = new Properties()
    props.setProperty("kylin.metadata.url", metaDataUrl.get)
    props.setProperty("kylin.query.security.acl-tcr-enabled", "false")
    KylinConfig.setKylinConfigInEnvIfMissing(props)
  }

  def validateEnvs(): Unit = {
    val sparkHomeDir = Option(System.getenv("SPARK_HOME"))
    assert(sparkHomeDir.isDefined, "SPARK_HOME need to be defined")
  }


  def setEnvs(options: scala.collection.mutable.HashMap[String, String]): Unit = {
    val hadoopConfDir = options.get("HADOOP_CONF_DIR")
    assert(hadoopConfDir.isDefined, "HADOOP_CONF_DIR need to be defined")
    hadoopConfDir.map(new File(_)).map(file => ClassUtil.addClasspath(file.getAbsolutePath))
  }

  def getValue(name: String, builder: Builder): Any = {
    val currentMirror = scala.reflect.runtime.currentMirror
    val instanceMirror = currentMirror.reflect(builder)
    val m = currentMirror
      .classSymbol(builder.getClass)
      .toType
      .members
      .find { p =>
        p.name.toString.equals(name)
      }
      .get
      .asTerm
    instanceMirror.reflectField(m).get
  }

}
