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

import java.lang.{Boolean => JBoolean, String => JString}
import java.security.PrivilegedAction
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Callable, ExecutorService}
import java.util.Map

import io.kyligence.kap.common.util.DefaultHostInfoFetcher
import io.kyligence.kap.metadata.model.NTableMetadataManager
import io.kyligence.kap.metadata.project.NProjectManager
import io.kyligence.kap.query.runtime.plan.QueryToExecutionIDCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kylin.common.exception.{KylinException, KylinTimeoutException, ServerErrorCode}
import org.apache.kylin.common.msg.MsgPicker
import org.apache.kylin.common.{KylinConfig, QueryContext}
import org.apache.kylin.common.util.{HadoopUtil, Pair, S3AUtil}
import org.apache.kylin.metadata.model.TableExtDesc
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerLogRollUp}
import org.apache.spark.sql.KylinSession._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasource.{KylinSourceStrategy, LayoutFileSourceStrategy}
import org.apache.spark.sql.execution.ui.PostQueryExecutionForKylin
import org.apache.spark.sql.udf.UdfManager
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

// scalastyle:off
object SparderEnv extends Logging {
  @volatile
  private var spark: SparkSession = _

  private val initializingLock = new ReentrantLock()
  private val initializingCondition = initializingLock.newCondition()
  private var initializing: Boolean = false
  private val initializingExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(1, "SparderEnv-Init")

  @volatile
  var APP_MASTER_TRACK_URL: String = null

  @volatile
  var startSparkFailureTimes: Int = 0

  @volatile
  var lastStartSparkFailureTime: Long = 0

  def getSparkSession: SparkSession = {
    if (spark == null || spark.sparkContext.isStopped) {
      logInfo("Init spark.")
      initSpark(() => doInitSpark())
    }
    if (spark == null)
      throw new KylinException(ServerErrorCode.SPARK_FAILURE, MsgPicker.getMsg.getSparkFailure)
    spark
  }

  def rollUpEventLog(): String = {
    if (spark != null && !spark.sparkContext.isStopped) {
      val check = "CHECK_ROLLUP_" + System.currentTimeMillis()
      spark.sparkContext.listenerBus.post(SparkListenerLogRollUp(check))
      return check
    }
    ""
  }

  def setSparkSession(sparkSession: SparkSession): Unit = {
    spark = sparkSession
    UdfManager.create(sparkSession)
  }

  def setAPPMasterTrackURL(url: String): Unit = {
    APP_MASTER_TRACK_URL = url
  }

  def isSparkAvailable: Boolean = {
    spark != null && !spark.sparkContext.isStopped
  }

  def restartSpark(): Unit = {
    this.synchronized {
      if (spark != null && !spark.sparkContext.isStopped) {
        Utils.tryWithSafeFinally {
          spark.stop()
        } {
          SparkContext.clearActiveContext
        }
      }

      logInfo("Restart Spark")
      init()
    }
  }

  def init(): Unit = {
    getSparkSession
  }

  def getSparkConf(key: String): String = {
    getSparkSession.sparkContext.conf.get(key)
  }

  def getTotalCore: Int = {
    val sparkConf = getSparkSession.sparkContext.getConf
    if (sparkConf.get("spark.master").startsWith("local")) {
      return 1
    }
    val instances = getExecutorNum(sparkConf)
    val cores = sparkConf.get("spark.executor.cores").toInt
    Math.max(instances * cores, 1)
  }

  def getExecutorNum(sparkConf: SparkConf): Int = {
    if (sparkConf.get("spark.dynamicAllocation.enabled", "false").toBoolean) {
      val maxExecutors = sparkConf.get("spark.dynamicAllocation.maxExecutors", Int.MaxValue.toString).toInt
      logInfo(s"Use spark.dynamicAllocation.maxExecutors:$maxExecutors as num instances of executors.")
      maxExecutors
    } else {
      sparkConf.get("spark.executor.instances").toInt
    }
  }

  def initSpark(doInit: () => Unit): Unit = {
    // do init
    try {
      initializingLock.lock()
      // exit if spark is running or it's during initializing
      if ((spark == null || spark.sparkContext.isStopped) && !initializing) {

        initializing = true

        initializingExecutor.submit(new Callable[Unit]() {
          override def call(): Unit = {
            try {
              logInfo("Initializing Spark thread starting.")
              doInit()
            } finally {
              logInfo("Initialized Spark")
              // wake up all waiting query threads after init done
              initializingLock.lock()
              initializing = false
              initializingCondition.signalAll()
              initializingLock.unlock()
            }
          }
        })
      }
    } finally {
      initializingLock.unlock()
    }

    // wait until initializing done
    try {
      initializingLock.lock()
      if (Thread.interrupted()) { // exit in case thread is interrupted already
        throw new InterruptedException
      }
      while (initializing) {
        initializingCondition.await()
      }
    } catch {
      case _: InterruptedException =>
        Thread.currentThread.interrupt()
        QueryContext.current().getQueryTagInfo.setTimeout(true)
        logWarning(s"Query timeouts after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s")
        throw new KylinTimeoutException("The query exceeds the set time limit of "
          + KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds + "s. Current step: Init sparder. ")
    } finally {
      initializingLock.unlock()
    }

    initConnWithHive()
  }

  private def initConnWithHive(): Unit = {
    try {
      UserGroupInformation.getLoginUser.doAs(new PrivilegedAction[Unit] {
        override def run(): Unit = spark.sql("show databases").show()
      })
    } catch {
      case throwable: Throwable =>
        logError("Error for initializing connection with hive.", throwable)
    }
  }

  def doInitSpark(): Unit = {
    try {
      val hostInfoFetcher = new DefaultHostInfoFetcher
      val appName = "sparder-" + UserGroupInformation.getCurrentUser.getShortUserName + "-" + hostInfoFetcher.getHostname

      val isLocalMode = KylinConfig.getInstanceFromEnv.isJobNodeOnly ||
        ("true").equals(System.getProperty("spark.local"))
      val sparkSession = isLocalMode match {
        case true =>
          SparkSession.builder
            .master("local")
            .appName("sparder-local-sql-context")
            .withExtensions { ext =>
              ext.injectPlannerStrategy(_ => KylinSourceStrategy)
              ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
            }
            .enableHiveSupport()
            .getOrCreateKylinSession()
        case _ =>
          SparkSession.builder
            .appName(appName)
            .master("yarn")
            //if user defined other master in kylin.properties,
            // it will get overwrite later in org.apache.spark.sql.KylinSession.KylinBuilder.initSparkConf
            .withExtensions { ext =>
            ext.injectPlannerStrategy(_ => KylinSourceStrategy)
            ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
          }
            .enableHiveSupport()
            .getOrCreateKylinSession()
      }
      spark = sparkSession
      logInfo("Spark context started successfully with stack trace:")
      logInfo(Thread.currentThread().getStackTrace.mkString("\n"))
      logInfo(
        "Class loader: " + Thread
          .currentThread()
          .getContextClassLoader
          .toString)
      registerListener(sparkSession.sparkContext)
      APP_MASTER_TRACK_URL = null
      startSparkFailureTimes = 0
      lastStartSparkFailureTime = 0

      //add s3 permission credential from tableExt
      if (KylinConfig.getInstanceFromEnv.useDynamicS3RoleCredentialInTable) {
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv).listAllProjects().forEach(project => {
          val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project.getName)
          tableMetadataManager.listAllTables().forEach(tableDesc => SparderEnv.addS3CredentialFromTableToSpark(tableMetadataManager.getOrCreateTableExt(tableDesc), spark))
        })
      }
    } catch {
      case throwable: Throwable =>
        logError("Error for initializing spark ", throwable)
        startSparkFailureTimes += 1
        lastStartSparkFailureTime = System.currentTimeMillis()
    }
  }

  def registerListener(sc: SparkContext): Unit = {
    val sparkListener = new SparkListener {

      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: PostQueryExecutionForKylin =>
          val queryID = e.localProperties.getProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, "")
          QueryToExecutionIDCache.setQueryExecutionID(queryID, e.executionId.toString)
          val executionID = e.localProperties.getProperty(QueryToExecutionIDCache.KYLIN_QUERY_EXECUTION_ID, "")
          QueryToExecutionIDCache.setQueryExecution(executionID, e.queryExecution)
        case _ => // Ignore
      }
    }
    sc.addSparkListener(sparkListener)
  }

  /**
   * @param sqlText SQL to be validated
   * @return The logical plan
   * @throws ParseException if validate failed
   */
  @throws[ParseException]
  def validateSql(sqlText: String): LogicalPlan = {
    val logicalPlan: LogicalPlan = getSparkSession.sessionState.sqlParser.parsePlan(sqlText)
    logicalPlan
  }

  val _separator = new ThreadLocal[JString]
  val _df = new ThreadLocal[Dataset[Row]]
  val _needCompute = new ThreadLocal[JBoolean] {
    override protected def initialValue = false
  }

  def setSeparator(separator: java.lang.String): Unit = _separator.set(separator)

  def getSeparator: java.lang.String = if (_separator.get == null) "," else _separator.get

  def getDF: Dataset[Row] = _df.get

  def setDF(df: Dataset[Row]): Unit = _df.set(df)

  // clean it after query end
  def clean(): Unit = {
    _df.remove()
    _needCompute.remove()
  }

  def needCompute(): JBoolean = {
    !_needCompute.get()
  }

  def skipCompute(): Unit = {
    _needCompute.set(true)
  }

  def cleanCompute(): Unit = {
    _needCompute.set(false)
  }

  def addS3CredentialFromTableToSpark(tableExtDesc: TableExtDesc, sparkSession: SparkSession): Unit = {
    val s3CredentialInfo = tableExtDesc.getS3RoleCredentialInfo
    if (s3CredentialInfo != null) {
      val conf: Map[String, String] = S3AUtil.generateRoleCredentialConfByBucketAndRoleAndEndpoint(s3CredentialInfo.getBucket, s3CredentialInfo.getRole, s3CredentialInfo.getEndpoint)
      conf.forEach((key: String, value: String) => sparkSession.conf.set(key, value))
    }

  }

  def getHadoopConfiguration(): Configuration = {
    var configuration = HadoopUtil.getCurrentConfiguration
    spark.conf.getAll.filter(item => item._1.startsWith("fs.")).foreach(item => configuration.set(item._1, item._2))
    configuration
  }


}
