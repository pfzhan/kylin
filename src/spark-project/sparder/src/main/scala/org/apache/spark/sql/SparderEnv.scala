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
import java.util.concurrent.atomic.AtomicReference

import io.kyligence.kap.query.runtime.plan.QueryToExecutionIDCache
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MonitorEnv
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerLogRollUp}
import org.apache.spark.sql.KylinSession._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasource.{KylinSourceStrategy, LayoutFileSourceStrategy}
import org.apache.spark.sql.execution.ui.PostQueryExecutionForKylin
import org.apache.spark.sql.udf.UdfManager
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

// scalastyle:off
object SparderEnv extends Logging {
  @volatile
  private var spark: SparkSession = _

  @volatile
  private var initializingThread: Thread = null

  @volatile
  var APP_MASTER_TRACK_URL: String = null

  @volatile
  var startSparkFailureTimes: Int = 0

  @volatile
  var lastStartSparkFailureTime: Long = 0

  def getSparkSession: SparkSession = withClassLoad {
    if (spark == null || spark.sparkContext.isStopped) {
      logInfo("Init spark.")
      initSpark()
    }
    spark
  }

  def rollUpEventLog(): String = {
    if (spark != null && !spark.sparkContext.isStopped) {
      val check ="CHECK_ROLLUP_" + System.currentTimeMillis()
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

  def restartSpark(): Unit = withClassLoad {
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

  def init(): Unit = withClassLoad {
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

  def initSpark(): Unit = withClassLoad {
    this.synchronized {
      if (initializingThread == null && (spark == null || spark.sparkContext.isStopped)) {
        initializingThread = new Thread(new Runnable {
          override def run(): Unit = {
            var startSparkSucceed = false

            try {
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
                    .appName("sparder-sql-context")
                    .master("yarn-client")
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
              initMonitorEnv()
              APP_MASTER_TRACK_URL = null
              startSparkSucceed = true
            } catch {
              case throwable: Throwable =>
                logError("Error for initializing spark ", throwable)
            } finally {
              if (startSparkSucceed) {
                startSparkFailureTimes = 0
                lastStartSparkFailureTime = 0
              } else {
                startSparkFailureTimes += 1
                lastStartSparkFailureTime = System.currentTimeMillis()
              }

              logInfo("Setting initializing Spark thread to null.")
              initializingThread = null
            }
          }
        })

        logInfo("Initializing Spark thread starting.")
        initializingThread.start()
      }

      if (initializingThread != null) {
        logInfo("Initializing Spark, waiting for done.")
        initializingThread.join()
      }

      try {
        UserGroupInformation.getLoginUser.doAs(new PrivilegedAction[Unit] {
          override def run(): Unit = spark.sql("show databases").show()
        })
      } catch {
        case throwable: Throwable =>
          logError("Error for initializing connection with hive.", throwable)
      }
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

  def initMonitorEnv(): Unit = {
    val env = SparkEnv.get
    val rpcEnv = env.rpcEnv
    val sparkConf = new SparkConf
    MonitorEnv.create(sparkConf, env.executorId, rpcEnv, null, isDriver = true)
    logInfo("setup master endpoint finished." + "hostPort:" + rpcEnv.address.hostPort)
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

  /**
    * To avoid spark being affected by the environment, we use spark classloader load spark.
    *
    * @param body Somewhere if you use spark
    * @tparam T Action function
    * @return The body return
    */
  def withClassLoad[T](body: => T): T = {
    //    val originClassLoad = Thread.currentThread().getContextClassLoader
    // fixme aron
    //        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader)
    val t = body
    //    Thread.currentThread().setContextClassLoader(originClassLoad)
    t
  }

  val _separator = new ThreadLocal[JString]
  val _df = new ThreadLocal[Dataset[Row]]
  val _needCompute = new ThreadLocal[JBoolean] {
    override protected def initialValue = false
  }

  //cleaned
  val _numScanFiles =
    new ThreadLocal[java.lang.Long] {
      override protected def initialValue = 0L
    }

  val _queryRef =
    new ThreadLocal[AtomicReference[java.lang.Boolean]]

  def accumulateScanFiles(numFiles: java.lang.Long): Unit = {
    _numScanFiles.set(_numScanFiles.get() + numFiles)
  }

  def getNumScanFiles(): java.lang.Long = {
    _numScanFiles.get()
  }

  def setSeparator(separator: java.lang.String): Unit = {
    _separator.set(separator)
  }

  def getSeparator: java.lang.String =
    if (_separator.get == null) ","
    else _separator.get

  def getDF: Dataset[Row] = _df.get

  def setDF(df: Dataset[Row]): Unit = {
    _df.set(df)
  }

  def setResultRef(ref: AtomicReference[java.lang.Boolean]): Unit = {
    _queryRef.set(ref)
  }

  def getResultRef: AtomicReference[java.lang.Boolean] = _queryRef.get

  // clean it after query end
  def clean(): Unit = {
    _separator.set(null)
    _df.set(null)
    _needCompute.set(null)
  }

  // clean it after collect
  def cleanQueryInfo(): Unit = {
    _numScanFiles.set(0L)
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

}
