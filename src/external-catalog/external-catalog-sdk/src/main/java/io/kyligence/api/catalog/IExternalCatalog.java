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
package io.kyligence.api.catalog;

import io.kyligence.api.ApiException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public interface IExternalCatalog {
    /**
     * Get all existing databases that match the given pattern.
     * The matching occurs as per Java regular expressions
     * @param databasePattern java re pattern
     * @return List of database names.
     * @throws ApiException
     */
    List<String> getDatabases(String databasePattern) throws ApiException;

    /**
     * Get a Database Object
     * @param databaseName name of the database to fetch
     * @return the database object
     * @throws ApiException
     */
    Database getDatabase(String databaseName) throws ApiException;

    /**
     * Returns metadata of the table
     * @param dbName the name of the database
     * @param tableName the name of the table
     * @param throwException controls whether an exception is thrown or a returns a null when table not found
     * @return the table or if throwException is false a null value.
     * @throws ApiException
     */
    Table getTable(String dbName, String tableName, boolean throwException) throws ApiException;

    /**
     * Returns all existing tables from the specified database which match the given
     * pattern. The matching occurs as per Java regular expressions.
     * @param dbName database name
     * @param tablePattern java re pattern
     * @return list of table names
     * @throws ApiException
     */
    List<String> getTables(String dbName, String tablePattern) throws ApiException;

    /**
     * Returns data of the table
     *
     * @param session spark session
     * @param dbName the name of the database
     * @param tableName the name of the table
     * @param throwException controls whether an exception is thrown or a returns a null when table not found
     * @return the table data or if throwException is false a null value.
     * @throws ApiException
     */
    Dataset<Row> getTableData(SparkSession session, String dbName, String tableName, boolean throwException)
            throws ApiException;

    /**
     * List the metadata of all partitions that belong to the specified table, assuming it exists.
     *
     * @param dbName database name
     * @param tablePattern table name
     *
     */
    List<Partition> listPartitions(String dbName, String tablePattern) throws ApiException;
}
