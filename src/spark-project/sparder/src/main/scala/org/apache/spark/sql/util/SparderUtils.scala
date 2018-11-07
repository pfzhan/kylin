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
package org.apache.spark.sql.util

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerInterface}
import org.apache.spark.sql.SparderEnv
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.storage.StorageListener

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparderUtils {
  def getListenersWithType: mutable.HashMap[String, SparkListenerInterface] = {
    if (!SparderEnv.isSparkAvailable) {
      return mutable.HashMap.empty
    }

    val session = SparderEnv.getSparkSession
    val field = classOf[LiveListenerBus].getDeclaredField("listeners")
    field.setAccessible(true)
    val listenersWithType =
      new mutable.HashMap[String, SparkListenerInterface]()

    val listeners = field
      .get(session.sparkContext.listenerBus)
      .asInstanceOf[CopyOnWriteArrayList[SparkListenerInterface]]
    listeners.asScala.foreach {
      case l: ExecutorsListener => listenersWithType.put("ExecutorsListener", l)
      case l: StorageStatusListener =>
        listenersWithType.put("StorageStatusListener", l)
      case l: EnvironmentListener =>
        listenersWithType.put("EnvironmentListener", l)
      case l: StorageListener => listenersWithType.put("StorageListener", l)
      case _ => Unit
    }
    listenersWithType
  }

/*
  def getSystemPropOrElse(prop: String, df: String): String = {
    val str = System.getProperty(prop)
    if (str == null || str.isEmpty) {
      df
    } else {
      str
    }
  }


  def fillAliasRelation(plan: LogicalPlan): KylinAliasDecoderRelation = {
    val aliasMap = KylinAliasDecoderRelation()
    plan transformAllExpressions {
      case a@Alias(exp, name) =>
        exp match {
          case attr: Attribute =>
            aliasMap.put(a.toAttribute, attr)
          case cast: Expression =>
            cast transformDown {
              case attr: AttributeReference =>
                aliasMap.put(a.toAttribute, attr.toAttribute)
                attr
            }
          case other =>
            other transformDown {
              case attr: AttributeReference =>
                aliasMap.put(a.toAttribute, attr.toAttribute)
                attr
            }
        }
        a
    }
    aliasMap
  }
*/
}
