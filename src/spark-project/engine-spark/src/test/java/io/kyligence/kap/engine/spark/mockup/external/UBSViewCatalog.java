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
package io.kyligence.kap.engine.spark.mockup.external;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Maps;

import io.kyligence.api.ApiException;
import io.kyligence.api.catalog.Database;
import io.kyligence.api.catalog.IExternalCatalog;
import io.kyligence.api.catalog.Partition;
import io.kyligence.api.catalog.Table;

public class UBSViewCatalog implements IExternalCatalog {

    static public final String VIEW_NAME = "x";
    static public final String DB_NAME = "UBSVIEWCATALOG";

    static private final Database DEFAULT =
            new Database("UBSVIEWCATALOG", "", "", Maps.newHashMap());

    static private final Table xTable = createDefaultTable();

    static private Table createDefaultTable() {
        Table t = new Table(VIEW_NAME, DB_NAME);
        t.setFields(Collections.emptyList());
        return t;
    }

    public UBSViewCatalog(Configuration hadoopConfig) {
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws ApiException {
        return Collections.singletonList(DB_NAME);
    }

    @Override
    public Database getDatabase(String databaseName) throws ApiException {
        return DEFAULT;
    }

    @Override
    public Table getTable(String dbName, String tableName, boolean throwException) throws ApiException {
        if (VIEW_NAME.equalsIgnoreCase(tableName)) {
            return xTable;
        } else {
            return null;
        }

    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws ApiException {
        return null;
    }

    @Override
    public Dataset<Row> getTableData(SparkSession session, String dbName, String tableName, boolean throwException) throws ApiException {
        if (tableName.equalsIgnoreCase(VIEW_NAME)) {
            String viewSql = "select id, sum(t0) from dim group by id";
            return session.sql(viewSql);
        }
        return null;
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tablePattern) throws ApiException {
        return null;
    }
}