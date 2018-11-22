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

import org.apache.hadoop.fs.Path

/**
 * [[IndexIdentifier]] describes index location in metastore by providing information about
 * metastore support identifier and dataspace for index, e.g. datasource or catalog table.
 */
abstract class IndexLocationSpec private[datasources] {

  /** Raw value for identifier, will be resolved on the first access */
  protected def unresolvedIndentifier: String

  /** Raw value for dataspace, will be resolved on the first access */
  protected def unresolvedDataspace: String

  /** Path to the source table that is backed by filesystem-based datasource */
  def sourcePath: Path

  /** Support string identifier */
  lazy val identifier: String = {
    validate(unresolvedIndentifier, "identifier")
    unresolvedIndentifier
  }

  /** Dataspace identifier, must be one of the supported spaces */
  lazy val dataspace: String = {
    validate(unresolvedDataspace, "dataspace")
    unresolvedDataspace
  }

  /** Validate property of the location spec */
  private def validate(value: String, property: String): Unit = {
    require(value != null && value.nonEmpty, s"Empty $property")
    value.foreach { ch =>
      require(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z', s"Invalid character $ch in " +
        s"$property $value. Only lowercase alpha-numeric characters are supported")
    }
  }

  override def toString(): String = {
    s"[$dataspace/$identifier, source=$sourcePath]"
  }
}

/** Location spec for datasource table */
private[datasources] case class SourceLocationSpec(
    unresolvedIndentifier: String,
    sourcePath: Path)
  extends IndexLocationSpec {

  override protected def unresolvedDataspace: String = "source"
}

/** Location spec for catalog (persisten) table */
private[datasources] case class CatalogLocationSpec(
    unresolvedIndentifier: String,
    sourcePath: Path)
  extends IndexLocationSpec {

  override protected def unresolvedDataspace: String = "catalog"
}
