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
package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.FSNamespaceUtils
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SQLConf, SessionState}

/**
 * hive session  hava some rule exp: find datasource table rule
 *
 * @param sparkSession
 * @param parentState
 */
class KylinHiveSessionStateBuilder(sparkSession: SparkSession,
                                   parentState: Option[SessionState] = None)
    extends HiveSessionStateBuilder(sparkSession, parentState) {

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  override protected def newBuilder: NewBuilder =
    new KylinHiveSessionStateBuilder(_, _)

}

case class ReplaceLocationRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) =>
      val specFS = sparkSession.sessionState.conf.getConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION)
      val specCatalog = FSNamespaceUtils.replaceLocWithSpecPrefix(specFS,
        relation.tableMeta.storage)
      val specTableMeta = relation.tableMeta.copy(storage = specCatalog)
      val specRelation = if (specFS != null && specCatalog.locationUri.isDefined) {
        relation.copy(tableMeta = specTableMeta)
      } else relation
      specRelation
  }
}

/**
 * use for no hive mode
 *
 * @param sparkSession
 * @param parentState
 */
class KylinSessionStateBuilder(sparkSession: SparkSession,
                               parentState: Option[SessionState] = None)
    extends BaseSessionStateBuilder(sparkSession, parentState) {

  override protected def newBuilder: NewBuilder =
    new KylinSessionStateBuilder(_, _)

}
