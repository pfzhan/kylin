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
package org.apache.spark.sql.connector.read.sqlpushdown;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources which support SQL can implement this
 *  interface to push down SQL to backend and reduce the size of the data to be read.
 *
 *  @since 3.x.x
 */

@Evolving
public interface SupportsSQLPushDown extends ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters {
  /**
   * Return true if executing a query on them would result in a query issued to multiple partitions.
   * Returns false if it would result in a query to a single partition and therefore provides global
   * results.
   */
  boolean isMultiplePartitionExecution();

  /**
   * Pushes down {@link SQLStatement} to datasource and returns filters that need to be evaluated
   * after scanning.
   * <p>
   * Rows should be returned from the data source if and only if all of the filters match. That is,
   * filters must be interpreted as ANDed together.
   */
  Filter[] pushStatement(SQLStatement statement, StructType outputSchema);

  /**
   * Returns the statement that are pushed to the data source via
   * {@link #pushStatement(SQLStatement statement, StructType outputSchema)}
   */
  SQLStatement pushedStatement();
}
