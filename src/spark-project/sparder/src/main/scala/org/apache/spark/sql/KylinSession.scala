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

package org.apache.spark.sql

import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.sql.SQLException

import io.kyligence.kap.metadata.project.NProjectManager
import io.kyligence.kap.query.engine.QueryExec
import io.kyligence.kap.query.util.ExtractFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.query.util.{QueryParams, QueryUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.sql.udf.UdfManager
import org.apache.spark.util.KylinReflectUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class KylinSession(
                    @transient val sc: SparkContext,
                    @transient private val existingSharedState: Option[SharedState])
  extends SparkSession(sc) {

  def this(sc: SparkContext) {
    this(sc, None)
  }

  @transient
  override lazy val sessionState: SessionState =
    KylinReflectUtils.getSessionState(sc, this).asInstanceOf[SessionState]

  override def newSession(): SparkSession = {
    new KylinSession(sparkContext, Some(sharedState))
  }

  def singleQuery(sql: String, project: String): DataFrame = {
    val prevRunLocalConf = System.setProperty("kylin.query.engine.run-constant-query-locally", "FALSE")
    try {
      val projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv).getProject(project).getConfig;
      val queryExec = new QueryExec(project, projectKylinConfig)
      val queryParams = new QueryParams(QueryUtil.getKylinConfig(project),
        sql, project, 0, 0, queryExec.getSchema, true)
      val convertedSql = QueryUtil.massageSql(queryParams)
      queryExec.executeQuery(convertedSql)
    } finally {
      if (prevRunLocalConf == null) {
        System.clearProperty("kylin.query.engine.run-constant-query-locally")
      } else {
        System.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf)
      }
    }
    SparderEnv.getDF
  }

  def sql(project: String, sqlText: String): DataFrame = {
    Try {
      singleQuery(sqlText, project)
    } match {
      case Success(result_df) =>
        result_df
      case Failure(e) =>
        if (e.isInstanceOf[SQLException]) {
          sql(sqlText)
        } else {
          throw e
        }
    }
  }

}

object KylinSession extends Logging {

  implicit class KylinBuilder(builder: Builder) {


    def getOrCreateKylinSession(): SparkSession = synchronized {
      val options =
        getValue("options", builder)
          .asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val userSuppliedContext: Option[SparkContext] =
        getValue("userSuppliedContext", builder)
          .asInstanceOf[Option[SparkContext]]
      var session: SparkSession = SparkSession.getActiveSession match {
        case Some(sparkSession: KylinSession) =>
          if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
            options.foreach {
              case (k, v) => sparkSession.sessionState.conf.setConfString(k, v)
            }
            sparkSession
          } else {
            null
          }
        case _ => null
      }
      if (session ne null) {
        return session
      }
      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = SparkSession.getDefaultSession match {
          case Some(sparkSession: KylinSession) =>
            if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
              sparkSession
            } else {
              null
            }
          case _ => null
        }
        if (session ne null) {
          return session
        }
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val conf = new SparkConf()
          options.foreach { case (k, v) => conf.set(k, v) }
          val sparkConf = initSparkConf(conf)
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession

          // KE-12678
          if(sc.master.startsWith("yarn")) {
            System.setProperty("spark.ui.proxyBase", "/proxy/" + sc.applicationId)
          }

          sc
        }
        session = new KylinSession(sparkContext)
        SparkSession.setDefaultSession(session)
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(
                                         applicationEnd: SparkListenerApplicationEnd): Unit = {
            SparkSession.setDefaultSession(null)
          }
        })
        UdfManager.create(session)
        session
      }
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

    private lazy val kapConfig: KapConfig = KapConfig.getInstanceFromEnv

    def initSparkConf(sparkConf: SparkConf): SparkConf = {
      if (sparkConf.getBoolean("user.kylin.session", false)) {
        return sparkConf
      }
      sparkConf.set("spark.amIpFilter.enabled", "false")
      if (!KylinConfig.getInstanceFromEnv.getChannel.equalsIgnoreCase("cloud")) {
        sparkConf.set("spark.executor.plugins", "org.apache.spark.memory.MonitorExecutorExtension")
      }
      // kerberos
      if (kapConfig.isKerberosEnabled) {
        sparkConf.set("spark.yarn.keytab", kapConfig.getKerberosKeytabPath)
        sparkConf.set("spark.yarn.principal", kapConfig.getKerberosPrincipal)
        sparkConf.set("spark.yarn.security.credentials.hive.enabled", "false")
      }

      if (UserGroupInformation.isSecurityEnabled) {
        sparkConf.set("hive.metastore.sasl.enabled", "true")
      }

      kapConfig.getSparkConf.asScala.foreach {
        case (k, v) =>
          sparkConf.set(k, v)
      }
      val instances = sparkConf.get("spark.executor.instances").toInt
      val cores = sparkConf.get("spark.executor.cores").toInt
      val sparkCores = instances * cores
      if (sparkConf.get("spark.sql.shuffle.partitions", "").isEmpty) {
        sparkConf.set("spark.sql.shuffle.partitions", sparkCores.toString)
      }
      sparkConf.set("spark.debug.maxToStringFields", "1000")
      sparkConf.set("spark.scheduler.mode", "FAIR")
      if (new File(
        KylinConfig.getKylinConfDir.getCanonicalPath + "/fairscheduler.xml")
        .exists()) {
        val fairScheduler = KylinConfig.getKylinConfDir.getCanonicalPath + "/fairscheduler.xml"
        sparkConf.set("spark.scheduler.allocation.file", fairScheduler)
      }

      if (kapConfig.isQueryEscapedLiteral) {
        sparkConf.set("spark.sql.parser.escapedStringLiterals", "true")
      }

      if (!"true".equalsIgnoreCase(System.getProperty("spark.local"))) {
        if (sparkConf.get("spark.master").startsWith("yarn")) {
          sparkConf.set("spark.yarn.dist.jars",
            KylinConfig.getInstanceFromEnv.getKylinJobJarPath)
          sparkConf.set("spark.yarn.dist.files", kapConfig.sparderFiles())
        } else {
          sparkConf.set("spark.jars", kapConfig.sparderJars)
          sparkConf.set("spark.files", kapConfig.sparderFiles())
        }

        val fileName = KylinConfig.getInstanceFromEnv.getKylinJobJarPath
        sparkConf.set("spark.executor.extraClassPath", Paths.get(fileName).getFileName.toString)

        val krb5conf = " -Djava.security.krb5.conf=./__spark_conf__/__hadoop_conf__/krb5.conf"
        val executorExtraJavaOptions =
          sparkConf.get("spark.executor.extraJavaOptions", "")
        var executorKerberosConf = ""
        if (kapConfig.isKerberosEnabled && kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.FI_PLATFORM)) {
          executorKerberosConf = krb5conf
        }
        sparkConf.set("spark.executor.extraJavaOptions",
          s"$executorExtraJavaOptions -Duser.timezone=${kapConfig.getKylinConfig.getTimeZone} $executorKerberosConf")

        val yarnAMJavaOptions =
          sparkConf.get("spark.yarn.am.extraJavaOptions", "")
        var amKerberosConf = ""
        if (kapConfig.isKerberosEnabled && kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.FI_PLATFORM)) {
          amKerberosConf = krb5conf
        }
        sparkConf.set("spark.yarn.am.extraJavaOptions",
          s"$yarnAMJavaOptions $amKerberosConf")
      }

      val eventLogEnabled = sparkConf.getBoolean("spark.eventLog.enabled", false)
      var logDir = sparkConf.get("spark.eventLog.dir", "")
      if (eventLogEnabled && !logDir.isEmpty) {
        logDir = ExtractFactory.create.getSparderEvenLogDir
        sparkConf.set("spark.eventLog.dir", logDir)
        val logPath = new Path(new URI(logDir).getPath)
        val fs = HadoopUtil.getWorkingFileSystem()
        if (!fs.exists(logPath)) {
          fs.mkdirs(logPath)
        }
      }

      sparkConf
    }

  }

}