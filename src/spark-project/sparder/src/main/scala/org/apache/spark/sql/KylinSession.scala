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

import io.kyligence.kap.common.util.Unsafe
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
import org.apache.spark.sql.internal.{SessionState, SharedState, StaticSQLConf}
import org.apache.spark.sql.kylin.external.{KylinSessionStateBuilder, KylinSharedState}
import org.apache.spark.sql.udf.UdfManager
import org.apache.spark.util.{KylinReflectUtils, Utils}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class KylinSession(
                    @transient val sc: SparkContext,
                    @transient private val existingSharedState: Option[SharedState],
                    @transient private val parentSessionState: Option[SessionState],
                    @transient override private[sql] val extensions: SparkSessionExtensions)
  extends SparkSession(sc) {
  def this(sc: SparkContext) {
    this(sc,
      existingSharedState = None,
      parentSessionState = None,
      extensions = null)
  }

  @transient
  override lazy val sharedState: SharedState = {

    val className = KylinConfig.getInstanceFromEnv.getExternalCatalogClass
    val loadExternal = KylinSharedState.checkExternalClass(className)

    if (loadExternal) {
      existingSharedState.getOrElse(new KylinSharedState(sc, className))
    } else {
      // see https://stackoverflow.com/questions/45935672/scala-why-cant-we-do-super-val
      // we can't call  super.sharedState, copy SparkSession#sharedState
      existingSharedState.getOrElse(new SharedState(sparkContext, Map.empty))
    }
  }

  @transient
  override lazy val sessionState: SessionState = {
    val loadExteral = sharedState.isInstanceOf[KylinSharedState]
    if (loadExteral) {
      new KylinSessionStateBuilder(this, parentSessionState).build()
    } else {
      KylinReflectUtils.getSessionState(sc, this, parentSessionState).asInstanceOf[SessionState]
    }
  }

  override def newSession(): KylinSession = {
    new KylinSession(sparkContext, Some(sharedState), parentSessionState = None, extensions)
  }

  override def cloneSession(): SparkSession = {
    val result = new KylinSession(
      sparkContext,
      Some(sharedState),
      Some(sessionState),
      extensions)
    result.sessionState // force copy of SessionState
    result
  }

  def singleQuery(sql: String, project: String): DataFrame = {
    val prevRunLocalConf = Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", "FALSE")
    try {
      val projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv).getProject(project).getConfig
      val queryExec = new QueryExec(project, projectKylinConfig)
      val queryParams = new QueryParams(QueryUtil.getKylinConfig(project),
        sql, project, 0, 0, queryExec.getSchema, true)
      val convertedSql = QueryUtil.massageSql(queryParams)
      queryExec.executeQuery(convertedSql)
    } finally {
      if (prevRunLocalConf == null) {
        Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally")
      } else {
        Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf)
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
    var queryCluster: Boolean = true

    def getOrCreateKylinSession(): SparkSession = synchronized {
      val options =
        getValue("options", builder)
          .asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val userSuppliedContext: Option[SparkContext] =
        getValue("userSuppliedContext", builder)
          .asInstanceOf[Option[SparkContext]]
      val extensions = getValue("extensions", builder)
        .asInstanceOf[SparkSessionExtensions]
      var (session, existingSharedState, parentSessionState) = SparkSession.getActiveSession match {
        case Some(sparkSession: KylinSession) =>
          if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
            options.foreach {
              case (k, v) => sparkSession.sessionState.conf.setConfString(k, v)
            }
            (sparkSession, None, None)
          } else if ((sparkSession ne null) && sparkSession.sparkContext.isStopped) {
            (null, Some(sparkSession.sharedState), Some(sparkSession.sessionState))
          } else {
            (null, None, None)
          }
        case _ => (null, None, None)
      }
      if (session ne null) {
        return session
      }

      // for testing only
      // discard existing shardState directly
      // use in case that external shard state needs to be set in UT
      if (Option(System.getProperty("kylin.spark.discard-shard-state")).getOrElse("false").toBoolean) {
        existingSharedState = None
        parentSessionState = None
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
          val sparkConf: SparkConf = if (queryCluster) {
            initSparkConf(conf)
          } else {
            conf
          }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession

          // KE-12678
          if (sc.master.startsWith("yarn")) {
            Unsafe.setProperty("spark.ui.proxyBase", "/proxy/" + sc.applicationId)
          }

          sc
        }
        applyExtensions(
          sparkContext.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty),
          extensions)
        session = new KylinSession(sparkContext, existingSharedState, parentSessionState, extensions)
        SparkSession.setDefaultSession(session)
        SparkSession.setActiveSession(session)
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
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
      if (sparkConf.getBoolean("user.kylin.session", defaultValue = false)) {
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
      val cartesianFactor = KylinConfig.getInstanceFromEnv.getCartesianPartitionNumThresholdFactor
      var cartesianPartitionThreshold = sparkCores * cartesianFactor
      val confThreshold = sparkConf.get("spark.sql.cartesianPartitionNumThreshold")
      if (confThreshold.nonEmpty && confThreshold.toInt >= 0) {
        cartesianPartitionThreshold = confThreshold.toInt
      }
      sparkConf.set("spark.sql.cartesianPartitionNumThreshold", cartesianPartitionThreshold.toString)
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
          // TODO Less elegant implementation.
          val applicationJar = KylinConfig.getInstanceFromEnv.getKylinJobJarPath
          val yarnDistJarsConf = "spark.yarn.dist.jars"
          val distJars = if (sparkConf.contains(yarnDistJarsConf)) {
            s"${sparkConf.get(yarnDistJarsConf)},$applicationJar"
          } else {
            applicationJar
          }
          sparkConf.set(yarnDistJarsConf, distJars)
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
        if (kapConfig.isKerberosEnabled && (kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.FI_PLATFORM)
          || kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.TDH_PLATFORM))) {
          executorKerberosConf = krb5conf
        }
        sparkConf.set("spark.executor.extraJavaOptions",
          s"$executorExtraJavaOptions -Duser.timezone=${kapConfig.getKylinConfig.getTimeZone} $executorKerberosConf")

        val yarnAMJavaOptions =
          sparkConf.get("spark.yarn.am.extraJavaOptions", "")
        var amKerberosConf = ""
        if (kapConfig.isKerberosEnabled && (kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.FI_PLATFORM)
          || kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.TDH_PLATFORM))) {
          amKerberosConf = krb5conf
        }
        sparkConf.set("spark.yarn.am.extraJavaOptions",
          s"$yarnAMJavaOptions $amKerberosConf")
      }

      if (KylinConfig.getInstanceFromEnv.getQueryMemoryLimitDuringCollect > 0L) {
        sparkConf.set("spark.sql.driver.maxMemoryUsageDuringCollect", KylinConfig.getInstanceFromEnv.getQueryMemoryLimitDuringCollect + "m")
      }

      val eventLogEnabled = sparkConf.getBoolean("spark.eventLog.enabled", defaultValue = false)
      var logDir = sparkConf.get("spark.eventLog.dir", "")
      if (eventLogEnabled && logDir.nonEmpty) {
        logDir = ExtractFactory.create.getSparderEvenLogDir()
        sparkConf.set("spark.eventLog.dir", logDir)
        val logPath = new Path(new URI(logDir).getPath)
        val fs = HadoopUtil.getWorkingFileSystem()
        if (!fs.exists(logPath)) {
          fs.mkdirs(logPath)
        }
      }

      if (kapConfig.getKylinConfig.asyncProfilingEnabled()) {
        val plugins = sparkConf.get("spark.plugins", "")
        if (plugins.isEmpty) {
          sparkConf.set("spark.plugins", "io.kyligence.kap.query.asyncprofiler.AsyncProfilerSparkPlugin")
        } else {
          sparkConf.set("spark.plugins", "io.kyligence.kap.query.asyncprofiler.AsyncProfilerSparkPlugin," + plugins)
        }
      }

      sparkConf
    }

    def buildCluster(): KylinBuilder = {
      queryCluster = false
      this
    }
  }

  /**
    * Copied from SparkSession.applyExtensions. So that KylinSession can load extensions through SparkConf.
    * <p/>
    * Initialize extensions for given extension classnames. The classes will be applied to the
    * extensions passed into this function.
    */
  private def applyExtensions(
                               extensionConfClassNames: Seq[String],
                               extensions: SparkSessionExtensions): SparkSessionExtensions = {
    extensionConfClassNames.foreach { extensionConfClassName =>
      try {
        val extensionConfClass = Utils.classForName(extensionConfClassName)
        val extensionConf = extensionConfClass.getConstructor().newInstance()
          .asInstanceOf[SparkSessionExtensions => Unit]
        extensionConf(extensions)
      } catch {
        // Ignore the error if we cannot find the class or when the class has the wrong type.
        case e@(_: ClassCastException |
                _: ClassNotFoundException |
                _: NoClassDefFoundError) =>
          logWarning(s"Cannot use $extensionConfClassName to configure session extensions.", e)
      }
    }
    extensions
  }

}
