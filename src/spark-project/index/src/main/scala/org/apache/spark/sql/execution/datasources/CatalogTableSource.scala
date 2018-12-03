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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.FileSourceScanExec

/** Catalog table info that is used to reconstruct data source */
sealed case class CatalogTableInfo(format: String,
                                   inputPath: String,
                                   metadata: Map[String, String])

/** Source for catalog tables */
case class CatalogTableSource(val metastore: Metastore,
                              val tableName: String,
                              val options: Map[String, String],
                              val mode: SaveMode = SaveMode.ErrorIfExists)
    extends Logging {
  // metadata keys to extract
  val FORMAT = "Format"
  val INPUT_PATHS = "InputPaths"
  // parse table identifier and build logical plan
  val tableIdent =
    metastore.session.sessionState.sqlParser.parseTableIdentifier(tableName)
  val plan = metastore.session.sessionState.catalog.lookupRelation(tableIdent)
  val info = executeSourcePlan(plan)
  logInfo(s"Catalog table info $info")

  private def executeSourcePlan(plan: LogicalPlan): CatalogTableInfo = {
    val qe = metastore.session.sessionState.executePlan(plan)
    qe.assertAnalyzed
    qe.sparkPlan match {
      case scanExec: FileSourceScanExec if scanExec.tableIdentifier.isDefined =>
        // format describes subclass of FileFormat, and reference is slightly different from
        // datasource API, also we expect only single path/directory
        require(scanExec.metadata.contains(FORMAT), s"$FORMAT for $scanExec")
        require(scanExec.relation.location.rootPaths.length == 1,
                s"Input paths for $scanExec")

        val format = scanExec.metadata(FORMAT)
        val inputPath = scanExec.relation.location.rootPaths.head.toString
        val extendedOptions = options + ("path" -> inputPath)
        CatalogTableInfo(format, inputPath, extendedOptions)
      case other =>
        throw new UnsupportedOperationException(s"$other")
    }
  }

  /** Convert table source into indexed datasource */
  def asDataSource: IndexedDataSource = {
    IndexedDataSource(metastore = metastore,
                      className = info.format,
                      mode = mode,
                      options = info.metadata,
                      catalogTable = Some(info))
  }
}
