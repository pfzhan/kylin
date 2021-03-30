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
package org.apache.spark.sql.kylin.external

import io.kyligence.api.catalog.IExternalCatalog
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState, SharedState}
import org.apache.spark.util.Utils

import scala.util.control.NonFatal

class KylinSharedState (
    sc: SparkContext,
    private val externalCatalogClass: String) extends SharedState(sc, Map.empty) {
  override lazy val externalCatalog: ExternalCatalogWithListener = {
    val exteranl =
      KylinSharedState.instantiateExteranlCatalog(externalCatalogClass, sc.hadoopConfiguration)
    val externalCatalog = new KylinExternalCatalog(sc.conf, sc.hadoopConfiguration, exteranl)

    val defaultDbDefinition = CatalogDatabase(
      SessionCatalog.DEFAULT_DATABASE,
      "default database",
      CatalogUtils.stringToURI(conf.get(WAREHOUSE_PATH)),
      Map())

    // Create a default database in internal MemorySession Catalog even IExternalCatalog instance has one.
    // There may be another Spark application creating default database at the same time, here we
    // set `ignoreIfExists = true` to avoid `DatabaseAlreadyExists` exception.
    externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)


    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener(new ExternalCatalogEventListener {
      override def onEvent(event: ExternalCatalogEvent): Unit = {
        sparkContext.listenerBus.post(event)
      }
    })
    wrapped
  }
}

class KylinSessionStateBuilder(
    session: SparkSession,
    state: Option[SessionState])
  extends BaseSessionStateBuilder(session, state, Map.empty) {

  override def build(): SessionState = {
    require(session.sharedState.isInstanceOf[KylinSharedState])
    super.build()
  }

  override protected lazy val catalog: KylinSessionCatalog = {
    val catalog = new KylinSessionCatalog(
      () => session.sharedState.externalCatalog,
      () => session.sharedState.globalTempViewManager,
      functionRegistry,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader,
      session)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }
  override protected def newBuilder: NewBuilder = new KylinSessionStateBuilder(_, _)
}

object KylinSharedState extends Logging {
  def checkExternalClass(className: String): Boolean = {
    try {
      className match {
        case "" => false
        case _ =>
          Utils.classForName(className)
          true
      }
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError =>
        logWarning( s"Can't load Kylin external catalog $className, use Spark default instead")
        false
    }
  }
  private def instantiateExteranlCatalog(
      className: String,
      hadoopConfig: Configuration): IExternalCatalog = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(hadoopConfig).asInstanceOf[IExternalCatalog]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }
}