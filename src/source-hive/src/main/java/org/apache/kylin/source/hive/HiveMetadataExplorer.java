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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.source.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;

import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class HiveMetadataExplorer implements ISourceMetadataExplorer, ISampleDataDeployer {

    IHiveClient hiveClient = HiveClientFactory.getHiveClient();

    @Override
    public List<String> listDatabases() throws Exception {
        return hiveClient.getHiveDbNames();
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        return hiveClient.getHiveTableNames(database);
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String tableName, String prj) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(config, prj);

        HiveTableMeta hiveTableMeta;
        try {
            hiveTableMeta = hiveClient.getHiveTableMeta(database, tableName);
        } catch (Exception e) {
            throw new RuntimeException("cannot get HiveTableMeta", e);
        }

        TableDesc tableDesc = metaMgr.getTableDesc(database + "." + tableName);

        // make a new TableDesc instance, don't modify the one in use
        if (tableDesc == null) {
            tableDesc = new TableDesc();
            tableDesc.setDatabase(database.toUpperCase(Locale.ROOT));
            tableDesc.setName(tableName.toUpperCase(Locale.ROOT));
            tableDesc.setUuid(UUID.randomUUID().toString());
            tableDesc.setLastModified(0);
        } else {
            tableDesc = new TableDesc(tableDesc);
        }

        if (hiveTableMeta.tableType != null) {
            tableDesc.setTableType(hiveTableMeta.tableType);
        }

        int columnNumber = hiveTableMeta.allColumns.size();
        List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);
        for (int i = 0; i < columnNumber; i++) {
            HiveTableMeta.HiveTableColumnMeta field = hiveTableMeta.allColumns.get(i);
            ColumnDesc cdesc = new ColumnDesc();
            cdesc.setName(field.name.toUpperCase(Locale.ROOT));
            // use "double" in kylin for "float"
            if ("float".equalsIgnoreCase(field.dataType)) {
                cdesc.setDatatype("double");
            } else {
                cdesc.setDatatype(field.dataType);
            }
            cdesc.setId(String.valueOf(i + 1));
            cdesc.setComment(field.comment);
            columns.add(cdesc);
        }
        tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));

        StringBuilder partitionColumnString = new StringBuilder();
        for (int i = 0, n = hiveTableMeta.partitionColumns.size(); i < n; i++) {
            if (i > 0)
                partitionColumnString.append(", ");
            partitionColumnString.append(hiveTableMeta.partitionColumns.get(i).name.toUpperCase(Locale.ROOT));
        }

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setUuid(UUID.randomUUID().toString());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        tableExtDesc.addDataSourceProp("location", hiveTableMeta.sdLocation);
        tableExtDesc.addDataSourceProp("owner", hiveTableMeta.owner);
        tableExtDesc.addDataSourceProp("last_access_time", String.valueOf(hiveTableMeta.lastAccessTime));
        tableExtDesc.addDataSourceProp("partition_column", partitionColumnString.toString());
        tableExtDesc.addDataSourceProp("total_file_size", String.valueOf(hiveTableMeta.fileSize));
        tableExtDesc.addDataSourceProp("total_file_number", String.valueOf(hiveTableMeta.fileNum));
        tableExtDesc.addDataSourceProp("hive_inputFormat", hiveTableMeta.sdInputFormat);
        tableExtDesc.addDataSourceProp("hive_outputFormat", hiveTableMeta.sdOutputFormat);
        tableExtDesc.addDataSourceProp("skip_header_line_count", String.valueOf(hiveTableMeta.skipHeaderLineCount));

        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public boolean checkDatabaseAccess(String database) throws Exception {
        return true;
    }

    @Override
    public boolean checkTablesAccess(Set<String> tables) {
        return true;
    }

    @Override
    public Set<String> getTablePartitions(String database, String table, String prj, String partitionCols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        hiveClient.executeHQL(generateCreateSchemaSql(database));
    }

    private String generateCreateSchemaSql(String schemaName) {
        return String.format(Locale.ROOT, "CREATE DATABASE IF NOT EXISTS %s", schemaName);
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        hiveClient.executeHQL(generateCreateTableSql(table));
    }

    private String[] generateCreateTableSql(TableDesc tableDesc) {

        String dropsql = "DROP TABLE IF EXISTS " + tableDesc.getIdentity();
        String dropsql2 = "DROP VIEW IF EXISTS " + tableDesc.getIdentity();

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE " + tableDesc.getIdentity() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + getHiveDataType((col.getDatatype())) + "\n");
        }

        ddl.append(")" + "\n");
        ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + "\n");
        ddl.append("STORED AS TEXTFILE");

        return new String[] { dropsql, dropsql2, ddl.toString() };
    }

    @Override
    public void loadSampleData(String tableName, String tmpDataDir) throws Exception {
        hiveClient.executeHQL(generateLoadDataSql(tableName, tmpDataDir));
    }

    private String generateLoadDataSql(String tableName, String tableFileDir) {
        return "LOAD DATA LOCAL INPATH '" + tableFileDir + "/" + tableName + ".csv' OVERWRITE INTO TABLE " + tableName;
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) throws Exception {
        hiveClient.executeHQL(generateCreateViewSql(viewName, origTableName));
    }

    private String[] generateCreateViewSql(String viewName, String tableName) {

        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String dropTable = "DROP TABLE IF EXISTS " + viewName;

        String createSql = ("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        return new String[] { dropView, dropTable, createSql };
    }

    private static String getHiveDataType(String javaDataType) {
        String hiveDataType = javaDataType.toLowerCase(Locale.ROOT).startsWith("varchar") ? "string" : javaDataType;
        hiveDataType = javaDataType.toLowerCase(Locale.ROOT).startsWith("integer") ? "int" : hiveDataType;

        return hiveDataType.toLowerCase(Locale.ROOT);
    }

}
