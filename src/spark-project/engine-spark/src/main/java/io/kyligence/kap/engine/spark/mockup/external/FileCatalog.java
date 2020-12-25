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

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.kyligence.api.ApiException;
import io.kyligence.api.catalog.Database;
import io.kyligence.api.catalog.FieldSchema;
import io.kyligence.api.catalog.IExternalCatalog;
import io.kyligence.api.catalog.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A simple (ephemeral) implementation of the external catalog.
 *
 * This is a dummy implementation that does not require setting up external systems.
 * It is intended for testing or exploration purposes only and should not be used
 * in production.
 */

@Slf4j
public class FileCatalog implements IExternalCatalog {

     private void init(Configuration hadoopConfig) {
        try {
            FileSystem fileSystem = FileSystem.get(hadoopConfig);
            KylinConfigExt ext = KylinConfigExt.createInstance(KylinConfig.getInstanceFromEnv(), Maps.newHashMap());

            Preconditions.checkArgument(!Strings.isNullOrEmpty(ext.getExternalCatalogClass()));
            String meta = ext.getOptional("kylin.NSparkDataSource.data.dir", null);
            readDatabases(fileSystem, meta)
                    .forEach((database, tables) -> initDatabase(fileSystem, meta, database, Lists.newArrayList(tables)));
        } catch (Exception e) {
            log.error("Failed to initialize FileCatalog", e);
            throw new IllegalStateException(e);
        }
    }

    private Map<String, Set<String>> readDatabases(FileSystem fileSystem, String meta) throws IOException {
        Path tableDescPath = new Path(meta + "/tableDesc");
        Map<String, Set<String>> databases = Maps.newHashMap();
        Preconditions.checkArgument(fileSystem.exists(tableDescPath),
                "%s doesn't exists",
                tableDescPath.toUri().getRawPath());
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(tableDescPath, false);
        while (it.hasNext()) {
            LocatedFileStatus fs = it.next();
            if (fs.isDirectory())
                continue;
            String fileName = fs.getPath().getName();
            String[] strings = fileName.split("\\.");
            if (strings.length == 3 && "json".equals(strings[2])) {
                String database = strings[0];
                String table = strings[1];
                if (databases.containsKey(database)) {
                    databases.get(database).add(table);
                } else {
                    databases.put(database, Sets.newHashSet(table));
                }
            }
        }
        databases.put("FILECATALOGUT", Sets.newHashSet());
        return databases;
    }

    private void initDatabase(
            FileSystem fileSystem,
            String meta,
            String database,
            List<String> tables) {

        String dbUpperCaseName = database.toUpperCase(Locale.ROOT);

        if (!catalog.containsKey(dbUpperCaseName)) {
            Database newDb = new Database(dbUpperCaseName, "", "", Maps.newHashMap());
            catalog.put(database.toUpperCase(Locale.ROOT), new DatabaseDesc(newDb));
        }

        try {
            for (String table: tables) {

                String tableUpperCaseName = table.toUpperCase(Locale.ROOT);
                if (catalog.get(dbUpperCaseName).tables.containsKey(tableUpperCaseName))
                    continue;

                Path tableDescPath = new Path(String.format(Locale.ROOT, "%s/tableDesc/%s.%s.json", meta, database, table));
                TableDesc tableDesc = JsonUtil.readValue(fileSystem.open(tableDescPath), TableDesc.class);

                Table tableObj = new Table(tableUpperCaseName, dbUpperCaseName);
                List<FieldSchema> schemas = Arrays.stream(tableDesc.getColumns())
                        .map(columnDesc -> new FieldSchema(columnDesc.getName(), columnDesc.getDatatype(), ""))
                        .collect(Collectors.toList());
                Path path =
                        toAbsolutePath(fileSystem, new Path(String.format(Locale.ROOT, "%s/%s.csv", meta, tableDesc.getIdentity())));
                tableObj.setFields(schemas);
                tableObj.getSd().setPath(path.toUri().toString());

                catalog.get(dbUpperCaseName).tables.put(tableUpperCaseName, tableObj);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    // Database name -> description
    private Map<String, DatabaseDesc> catalog = Maps.newHashMap();

    private static class DatabaseDesc {
        Database db;
        Map<String, Table> tables = new HashMap<>();

        public DatabaseDesc(Database db) {
            this.db = db;
        }
    }

    private void requireDbExists(String db) throws ApiException {
        if (!catalog.containsKey(db.toUpperCase(Locale.ROOT))) {
            throw new ApiException(); // Error NoSuchDatabaseException
        }
    }

    private void requireTableExists(String db, String table) throws ApiException {
        requireDbExists(db);
        if(!catalog.get(db.toUpperCase(Locale.ROOT)).tables.containsKey(table.toUpperCase(Locale.ROOT))) {
            throw new ApiException(); // Error NoSuchTableException
        }
    }

    static Path toAbsolutePath(FileSystem fileSystem, Path path) throws IOException {

        if(path.isAbsolute())
            return path;

        try {
            return fileSystem.getFileStatus(path).getPath();
        } catch (FileNotFoundException ignored) {
            log.warn("{} does not exist! create empty table", path.toUri().toString());
        }
        return path;
    }

    //TODO: Refactor.
    // The plugin should load configuration by itself, the user can only assume ${KYLIN_HOME} is set
    public FileCatalog(Configuration hadoopConfig) {
        init(hadoopConfig);
    }

    public List<String> getDatabases(String databasePattern) throws ApiException {
        //TODO: support pattern
        return new ArrayList<>(catalog.keySet());
    }

    @Override
    public Database getDatabase(String databaseName) throws ApiException {
        DatabaseDesc desc = catalog.get(databaseName.toUpperCase(Locale.ROOT));
        if (desc == null)
            return null;
        return desc.db;
    }

    @Override
    public Table getTable(String dbName, String tableName, boolean throwException) throws ApiException {
        try {
            requireTableExists(dbName, tableName);
            return catalog.get(dbName.toUpperCase(Locale.ROOT)).tables.get(tableName.toUpperCase(Locale.ROOT));
        } catch (ApiException e) {
            if(throwException) {
                throw e;
            }
        }
        return null;
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws ApiException {
        requireDbExists(dbName);
        //TODO: support pattern
        return new ArrayList<>(catalog.get(dbName.toUpperCase(Locale.ROOT)).tables.keySet());
    }

    @Override
    public Dataset<Row> getTableData(
            SparkSession session,
            String dbName,
            String tableName,
            boolean throwException) throws ApiException {
        Table table = getTable(dbName, tableName, throwException);
        if (table == null || table.getSd().getPath() == null)
            return null;
        String schema = table.getFields()
                .stream()
                .map(s->s.getName() + " " + s.getType())
                .collect(Collectors.joining(","));
        return session.read().schema(schema).csv(table.getSd().getPath());
    }
}
