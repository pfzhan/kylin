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
package io.kyligence.kap.engine.spark.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;

public class NSparkMetadataExplorer implements ISourceMetadataExplorer, ISampleDataDeployer, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkMetadataExplorer.class);

    public NSparkTableMetaExplorer getTableMetaExplorer() {
        return new NSparkTableMetaExplorer();
    }

    @Override
    public List<String> listDatabases() throws Exception {
        Dataset<Row> dataset = SparderEnv.getSparkSession().sql("show databases").select("databaseName");
        return dataset.collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        val ugi = UserGroupInformation.getCurrentUser();
        val config = KylinConfig.getInstanceFromEnv();
        val spark = SparderEnv.getSparkSession();

        List<String> tables = Lists.newArrayList();
        try {
            String sql = "show tables";
            if (StringUtils.isNotBlank(database)) {
                sql = String.format(Locale.ROOT, sql + " in %s", database);
            }
            Dataset<Row> dataset = SparderEnv.getSparkSession().sql(sql).select("tableName");
            tables = dataset.collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());

            if (config.getKerberosProjectLevelEnable() && UserGroupInformation.isSecurityEnabled()) {
                List<String> accessTables = Lists.newArrayList();
                for (String table : tables) {
                    val tableName = database + "." + table;
                    if (checkTableAccess(tableName)) {
                        accessTables.add(table);
                    }
                }
                return accessTables;
            }
        } catch (Exception e) {
            logger.error("List hive tables failed. user: {}, db: {}", ugi.getUserName(), database);
        }

        return tables;
    }

    private boolean checkTableAccess(String tableName) {
        boolean isAccess = true;
        try {
            val spark = SparderEnv.getSparkSession();
            val sparkTable = spark.catalog().getTable(tableName);
            Set<String> needCheckTables = Sets.newHashSet();
            if (sparkTable.tableType().equals(CatalogTableType.VIEW().name())) {
                needCheckTables = SparkSqlUtil.getViewOrignalTables(tableName, SparderEnv.getSparkSession());
            } else {
                needCheckTables.add(tableName);
            }

            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            for (String table : needCheckTables) {
                String loc = spark.sql("desc formatted " + table).where("col_name == 'Location'").head().getString(1);
                fs.listStatus(new Path(loc));
            }
        } catch (Exception e) {
            isAccess = false;
            try {
                logger.error("Read hive table {} error, ugi name: {}.", tableName,
                        UserGroupInformation.getCurrentUser().getUserName());
            } catch (IOException ex) {
                logger.error("fetch user curr ugi info error.", e);
            }
        }
        return isAccess;
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(final String database, String tableName, String prj)
            throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(config, prj);

        NSparkTableMeta tableMeta = getTableMetaExplorer().getSparkTableMeta(database, tableName);
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

        if (tableMeta.tableType != null) {
            tableDesc.setTableType(tableMeta.tableType);
        }
        //set table type = spark
        tableDesc.setSourceType(ISourceAware.ID_SPARK);
        int columnNumber = tableMeta.allColumns.size();
        List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);
        for (int i = 0; i < columnNumber; i++) {
            NSparkTableMeta.SparkTableColumnMeta field = tableMeta.allColumns.get(i);
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
        List<String> partCols = tableMeta.partitionColumns.stream().map(col -> col.name).collect(Collectors.toList());
        if (!partCols.isEmpty()) {
            tableDesc.setPartitionColumn(partCols.get(0).toUpperCase(Locale.ROOT));
        } else {
            tableDesc.setPartitionColumn(null);
        }
        StringBuilder partitionColumnBuilder = new StringBuilder();
        for (int i = 0, n = tableMeta.partitionColumns.size(); i < n; i++) {
            if (i > 0)
                partitionColumnBuilder.append(", ");
            partitionColumnBuilder.append(tableMeta.partitionColumns.get(i).name.toUpperCase(Locale.ROOT));
        }

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setUuid(UUID.randomUUID().toString());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        tableExtDesc.addDataSourceProp("location", tableMeta.sdLocation);
        tableExtDesc.addDataSourceProp("owner", tableMeta.owner);
        tableExtDesc.addDataSourceProp("create_time", tableMeta.createTime);
        tableExtDesc.addDataSourceProp("last_access_time", tableMeta.lastAccessTime);
        tableExtDesc.addDataSourceProp("partition_column", partitionColumnBuilder.toString());
        tableExtDesc.addDataSourceProp("total_file_size", String.valueOf(tableMeta.fileSize));
        tableExtDesc.addDataSourceProp("total_file_number", String.valueOf(tableMeta.fileNum));
        tableExtDesc.addDataSourceProp("hive_inputFormat", tableMeta.sdInputFormat);
        tableExtDesc.addDataSourceProp("hive_outputFormat", tableMeta.sdOutputFormat);
        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public boolean checkDatabaseAccess(String database) throws Exception {
        boolean hiveDBAccessFilterEnable = KapConfig.getInstanceFromEnv().getDBAccessFilterEnable();
        if (hiveDBAccessFilterEnable) {
            logger.info("Check database {} access start.", database);
            try {
                Database db = SparderEnv.getSparkSession().catalog().getDatabase(database);
            } catch (AnalysisException e) {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                logger.info("The current user: {} does not have permission to access database {}", ugi.getUserName(),
                        database);
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean checkTablesAccess(Set<String> tables) {
        return tables.stream().allMatch(this::checkTableAccess);
    }

    @Override
    public Set<String> getTablePartitions(String database, String table, String prj, String partCol) {
        return getTableMetaExplorer().checkAndGetTablePartitions(database, table, partCol);
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        SparderEnv.getSparkSession().sql(generateCreateSchemaSql(database));
    }

    public static String generateCreateSchemaSql(String schemaName) {
        return String.format(Locale.ROOT, "CREATE DATABASE IF NOT EXISTS %s", schemaName);
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        String[] createTableSqls = generateCreateTableSql(table);
        for (String sql : createTableSqls) {
            SparderEnv.getSparkSession().sql(sql);
        }
    }

    public static String[] generateCreateTableSql(TableDesc tableDesc) {
        String dropSql = "DROP TABLE IF EXISTS " + tableDesc.getIdentity();

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE " + tableDesc.getIdentity() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + col.getDatatype() + "\n");
        }

        ddl.append(")" + "\n");
        ddl.append("USING com.databricks.spark.csv");

        return new String[] { dropSql, ddl.toString() };
    }

    @Override
    public void loadSampleData(String tableName, String tableFileDir) throws Exception {
        Dataset<Row> dataset = SparderEnv.getSparkSession().read().csv(tableFileDir + "/" + tableName + ".csv").toDF();
        if (tableName.indexOf(".") > 0) {
            tableName = tableName.substring(tableName.indexOf(".") + 1);
        }
        dataset.createOrReplaceTempView(tableName);
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) throws Exception {
        throw new UnsupportedOperationException("unsupport create wrapper view");
    }
}
