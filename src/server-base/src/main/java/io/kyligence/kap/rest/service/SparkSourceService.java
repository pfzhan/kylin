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
package io.kyligence.kap.rest.service;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DDLDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DdlOperation;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.rest.request.DDLRequest;
import io.kyligence.kap.rest.response.DDLResponse;
import io.kyligence.kap.rest.response.ExportTablesResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import lombok.Data;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.Option;
import scala.collection.Iterator;

@Slf4j
@Service
public class SparkSourceService extends BasicService {

    //key in hive metadata map
    private static final String HIVE_TYPE_STRING = "HIVE_TYPE_STRING";
    private static final String HIVE_COMMENT = "comment";

    public DDLResponse executeSQL(DDLRequest request) {
        List<String> sqlList = Arrays.asList(request.getSql().split(";"));
        if (!Strings.isNullOrEmpty(request.getDatabase())) {
            executeSQL("use " + request.getDatabase());
        }
        DDLResponse ddlResponse = new DDLResponse();
        Map<String, DDLDesc> succeed = Maps.newHashMap();
        Map<String, String> failed = Maps.newHashMap();
        sqlList.forEach(s -> {
            if (!Strings.isNullOrEmpty(s)) {
                try {
                    DDLDesc ddlDesc = executeSQL(s);
                    succeed.put(s, ddlDesc);
                } catch (Exception e) {
                    log.error("Failed to execute sql[{}]", s, e);
                    failed.put(s, e.getMessage());
                }
            }
        });
        ddlResponse.setSucceed(succeed);
        ddlResponse.setFailed(failed);
        return ddlResponse;
    }

    public DDLDesc executeSQL(String sql) {
        return DdlOperation.executeSQL(sql);
    }

    public void dropTable(String database, String table) throws AnalysisException {
        SparkSession ss = SparderEnv.getSparkSession();
        if (ss.catalog().tableExists(database, table)) {
            val t = ss.catalog().getTable(database, table);
            if ("view".equalsIgnoreCase(t.tableType())) {
                ss.sql(String.format("drop view %s.%s", database, table));
            } else {
                ss.sql(String.format("drop table %s.%s", database, table));
            }
        }
    }

    public List<String> listDatabase() {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        Dataset<Database> databaseSet = sparkSession.catalog().listDatabases();
        List<Database> databases = databaseSet.collectAsList();
        return databases.stream().map(database -> database.name().toUpperCase()).collect(Collectors.toList());
    }

    public List<TableNameResponse> listTables(String db, String project) throws Exception {
        if (Strings.isNullOrEmpty(project)) {
            SparkSession sparkSession = SparderEnv.getSparkSession();
            Dataset<Table> tableDataset = sparkSession.catalog().listTables(db);
            List<Table> sparkTables = tableDataset.collectAsList();
            return sparkTables.stream().map(table -> new TableNameResponse(table.name().toUpperCase(), false))
                    .collect(Collectors.toList());
        }
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        List<String> tables = explr.listTables(db).stream().map(String::toUpperCase).collect(Collectors.toList());
        List<TableNameResponse> tableNameResponses = Lists.newArrayList();
        tables.forEach(table -> {
            TableNameResponse response = new TableNameResponse();
            response.setLoaded(getTableManager(project).getTableDesc(db + "." + table) != null);
            response.setTableName(table);
            tableNameResponses.add(response);
        });
        return tableNameResponses;
    }

    public List<ColumnModel> listColumns(String db, String table) {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        CatalogTable catalogTable = sparkSession.sessionState().catalog()
                .getTempViewOrPermanentTableMetadata(new TableIdentifier(table, Option.apply(db)));
        scala.collection.immutable.List<StructField> structFieldList = catalogTable.schema().toList();
        Iterator<StructField> structFieldIterator = structFieldList.iterator();

        List<ColumnModel> columnModels = Lists.newArrayList();
        while (structFieldIterator.hasNext()) {
            StructField structField = structFieldIterator.next();
            String name = structField.name();
            String datatype = structField.dataType().simpleString();

            Metadata metadata = structField.metadata();

            ColumnModel columnModel = new ColumnModel();
            if (catalogTable.partitionColumnNames().contains(name)) {
                columnModel.setPartition(true);
            }
            columnModel.setName(name);
            columnModel.setDescription(metadata.contains(HIVE_COMMENT) ? metadata.getString(HIVE_COMMENT) : "");
            //use hive datatype if it exists , otherwise use spark datatype
            columnModel
                    .setDataType(metadata.contains(HIVE_TYPE_STRING) ? metadata.getString(HIVE_TYPE_STRING) : datatype);
            columnModels.add(columnModel);
        }

        return columnModels;
    }

    public String getTableDesc(String database, String table) {
        return DdlOperation.getTableDesc(database, table);
    }

    public ExportTablesResponse exportTables(String database, String[] tables) {
        if (database == null || database.equals("")) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, MsgPicker.getMsg().getEMPTY_DATABASE());
        }
        if (tables.length == 0) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, MsgPicker.getMsg().getEMPTY_TABLE_LIST());
        }
        if (!databaseExists(database)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(MsgPicker.getMsg().getDATABASE_NOT_EXIST(), database));
        }
        val tableResponse = new ExportTablesResponse();
        Map<String, String> tableDesc = Maps.newHashMap();
        for (String table : tables) {
            if (!tableExists(database, table)) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                        String.format(MsgPicker.getMsg().getTABLE_NOT_FOUND(), table));
            }
            tableDesc.put(table, DdlOperation.getTableDesc(database, table));
        }
        tableResponse.setDatabases(database);
        tableResponse.setTables(tableDesc);
        return tableResponse;
    }

    public boolean databaseExists(String database) {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        return sparkSession.catalog().databaseExists(database);
    }

    public boolean tableExists(String database, String table) {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        return sparkSession.catalog().tableExists(database, table);
    }

    public boolean hasPartition(String database, String table) {
        return DdlOperation.hasPartition(database, table);
    }

    public List<String> msck(String database, String table) {
        return DdlOperation.msck(database, table);
    }

    public List<String> loadSamples(SparkSession ss, SaveMode mode) throws IOException {
        //list samples and use file-name as table name
        List<File> fileList = listSampleFiles();
        List<String> createdTables = Lists.newArrayList();
        ss.catalog().setCurrentDatabase("DEFAULT");
        for (File file : fileList) {
            if (!file.isDirectory()) {
                continue;
            }
            String fileName = file.getName();
            String filePath = file.getAbsolutePath();
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
            String hdfsPath = String.format("/tmp/%s", fileName);
            try {
                log.debug("Copy from {} to {}", filePath, hdfsPath);
                File[] parquetFiles = file.listFiles();
                if (parquetFiles != null) {
                    for (File parquetFile : parquetFiles) {
                        fileSystem.copyFromLocalFile(new Path(parquetFile.getAbsolutePath()), new Path(hdfsPath));
                    }
                }
                ss.read().parquet(hdfsPath).write().mode(mode).saveAsTable(fileName);
            } catch (Exception e) {
                log.error("Load sample {} failed.", fileName, e);
                throw new IllegalStateException(String.format("Load sample %s failed", fileName), e);
            } finally {
                fileSystem.delete(new Path(hdfsPath), false);
            }
            String tableName = String.format("DEFAULT.%s", fileName.toUpperCase());
            createdTables.add(tableName);
        }
        log.info("Load samples {} successfully", StringUtil.join(createdTables, ","));
        return createdTables;
    }

    public List<String> loadSamples() throws IOException {
        log.info("Start to load samples");
        SparkSession ss = SparderEnv.getSparkSession();
        return loadSamples(ss, SaveMode.Overwrite);
    }

    private List<File> listSampleFiles() {
        //class_path samples
        String sampleDir = "../samples";
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            sampleDir = "../../build/samples";
        }
        File file = new File(sampleDir);
        log.debug("Samples file path is {}", file.getAbsolutePath());
        File[] listFiles = file.listFiles();
        if (!file.exists() || null == listFiles) {
            throw new RuntimeException("No sample data found.");
        }
        return Arrays.asList(listFiles);
    }

    @Data
    static class ColumnModel {

        @JsonProperty("name")
        private String name;
        @JsonProperty("description")
        private String description;
        @JsonProperty("dataType")
        private String dataType;
        @JsonProperty("partition")
        private boolean partition;
    }
}
