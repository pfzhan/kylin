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
package io.kyligence.kap.source.jdbc;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcExplorer implements ISourceMetadataExplorer, ISampleDataDeployer, Serializable {

    private JdbcConnector dataSource;

    public JdbcExplorer(JdbcConnector dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        String[] sql = dataSource.buildSqlToCreateSchema(database);
        dataSource.executeUpdate(sql);
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        LinkedHashMap<String, String> columnInfo = Maps.newLinkedHashMap();
        for (ColumnDesc columnDesc : table.getColumns()) {
            columnInfo.put(columnDesc.getName(), columnDesc.getTypeName());
        }
        String[] sqls = dataSource.buildSqlToCreateTable(table.getIdentity(), columnInfo);
        dataSource.executeUpdate(sqls);
    }

    @Override
    public void loadSampleData(String tableName, String tmpDataDir) {
        throw new UnsupportedOperationException("Unsupported load sample data");
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) {
        throw new UnsupportedOperationException("Unsupported create wrapper view");
    }

    @Override
    public List<String> listDatabases() throws Exception {
        return dataSource.listDatabases();
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        return dataSource.listTables(database);
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(config, prj);
        TableDesc tableDesc = metaMgr.getTableDesc(database + "." + table);
        // make a new TableDesc instance, don't modify the one in use
        if (tableDesc == null) {
            tableDesc = new TableDesc();
            tableDesc.setDatabase(database);
            tableDesc.setName(table);
            tableDesc.setLastModified(0);
        } else {
            tableDesc = new TableDesc(tableDesc);
        }

        tableDesc.setSourceType(ISourceAware.ID_JDBC);
        tableDesc.init(prj);

        CachedRowSet tables = dataSource.getTable(database, table);
        String tableType = null;
        while (tables.next()) {
            tableType = tables.getString("TABLE_TYPE");
        }
        if (tableType != null) {
            tableDesc.setTableType(tableType);
        } else {
            throw new RuntimeException(String.format(Locale.ROOT, "table %s not found in schema:%s", table, database));
        }

        CachedRowSet columns = dataSource.listColumns(database, table);
        List<ColumnDesc> columnDescs = Lists.newArrayList();
        while (columns.next()) {
            String cname = columns.getString("COLUMN_NAME");
            int type = columns.getInt("DATA_TYPE");
            int csize = columns.getInt("COLUMN_SIZE");
            int digits = columns.getInt("DECIMAL_DIGITS");
            int pos = columns.getInt("ORDINAL_POSITION");
            String remarks = columns.getString("REMARKS");

            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName(cname.toUpperCase(Locale.ROOT));
            columnDesc.setCaseSensitiveName(cname);
            String kylinType = dataSource.toKylinTypeName(type);
            if ("any".equals(kylinType)) {
                String typeName = columns.getString("TYPE_NAME");
                int kylinTypeId = dataSource.toKylinTypeId(typeName, type);
                kylinType = dataSource.toKylinTypeName(kylinTypeId);
            }
            int precision = (isPrecisionApplicable(kylinType) && csize > 0) ? csize : -1;
            precision = Math.min(precision, KylinConfig.getInstanceFromEnv().getDefaultVarcharPrecision());
            int scale = (isScaleApplicable(kylinType) && digits > 0) ? digits : -1;
            columnDesc.setDatatype(new DataType(kylinType, precision, scale).toString());
            columnDesc.setId(String.valueOf(pos));
            columnDesc.setComment(remarks);
            columnDescs.add(columnDesc);
        }

        tableDesc.setColumns(columnDescs.toArray(new ColumnDesc[columnDescs.size()]));

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public boolean checkDatabaseAccess(String database) {
        return true;
    }

    @Override
    public boolean checkTablesAccess(Set<String> tables) {
        return true;
    }

    public static boolean isPrecisionApplicable(String typeName) {
        return isScaleApplicable(typeName) || DataType.STRING_FAMILY.contains(typeName);
    }

    public static boolean isScaleApplicable(String typeName) {
        return typeName.equals("decimal") || typeName.equals("numeric"); // double and float are not allowed neither in hive.
    }
}
