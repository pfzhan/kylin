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

import java.util.concurrent.atomic.AtomicReference
import java.lang.{
  Boolean => JBoolean,
  Integer => JInteger,
  Long => JLong,
  String => JString
}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.KylinSession._
import org.apache.spark.sql.udf.UdfManager

// fixme aron: to be same with old KE
// scalastyle:off
object SparderEnv extends Logging {
  @volatile
  private var spark: SparkSession = _

  def getSparkSession: SparkSession = withClassLoad {
    if (spark == null || spark.sparkContext.isStopped) {
      logInfo("Init spark.")
      initSpark()
    }
    spark
  }

  def setSparkSession(sparkSession: SparkSession): Unit = {
    spark = sparkSession
    UdfManager.create(sparkSession)
  }

  def isSparkAvailable: Boolean = {
    spark != null && !spark.sparkContext.isStopped
  }

  def restartSpark(): Unit = withClassLoad {
    this.synchronized {
      if (spark != null && !spark.sparkContext.isStopped) {
        spark.stop()
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
    if(sparkConf.get("spark.master").startsWith("local")){
      return 1
    }
    val instances = sparkConf.get("spark.executor.instances").toInt
    val cores = sparkConf.get("spark.executor.cores").toInt
    Math.max(instances * cores, 1)
  }

  def initSpark(): Unit = withClassLoad {
    this.synchronized {
      if (spark == null || spark.sparkContext.isStopped) {
        try {
          val sparkSession = System.getProperty("spark.local") match {
            case "true" =>
              SparkSession.builder
                .master("local")
                .appName("sparder-test-sql-context")
                .enableHiveSupport()
                .getOrCreateKylinSession()
            case _ =>
              SparkSession.builder
                .appName("sparder-sql-context")
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
        } catch {
          case throwable: Throwable =>
            logError("Error for init spark ", throwable)
        }
      }
    }
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

  val _isAsyncQuery = new ThreadLocal[JBoolean]
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

  def setAsAsyncQuery(): Unit = {
    _isAsyncQuery.set(true)
  }

  def isAsyncQuery: java.lang.Boolean =
    if (_isAsyncQuery.get == null) false
    else _isAsyncQuery.get

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
    _isAsyncQuery.set(null)
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
