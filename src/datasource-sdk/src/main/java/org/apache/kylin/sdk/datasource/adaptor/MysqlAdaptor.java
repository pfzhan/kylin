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
package org.apache.kylin.sdk.datasource.adaptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

public class MysqlAdaptor extends DefaultAdaptor {
    public MysqlAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = getConnection()) {
            ret.add(con.getCatalog());
        }
        return ret;
    }

    @Override
    public List<String> listTables(String catalog) throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = getConnection(); ResultSet res = con.getMetaData().getTables(catalog, null, null, null)) {
            String table;
            while (res.next()) {
                table = res.getString("TABLE_NAME");
                ret.add(table);
            }
        }
        return ret;
    }

    @Override
    public CachedRowSet getTable(String catalog, String table) throws SQLException {
        if(configurer.isCaseSensitive()){
            catalog = getRealCatalogName(catalog);
            table = getRealTableName(catalog, table);
        }
        try (Connection conn = getConnection();ResultSet rs = conn.getMetaData().getTables(catalog, null, table, null)) {
            return cacheResultSet(rs);
        }
    }

    @Override
    public CachedRowSet getTableColumns(String catalog, String table) throws SQLException {
        if(configurer.isCaseSensitive()){
            catalog = getRealCatalogName(catalog);
            table = getRealTableName(catalog, table);
        }
        try (Connection conn = getConnection();
             ResultSet rs = conn.getMetaData().getColumns(catalog, null, table, null)) {
            return cacheResultSet(rs);
        }
    }

    private String getRealCatalogName(String catalog) throws SQLException {
        List<String> catalogs = super.listDatabasesWithCache();
        for (String s : catalogs) {
            if (s.equalsIgnoreCase(catalog)) {
                catalog = s;
                break;
            }
        }
        return catalog;
    }

    private String getRealTableName(String catalog, String table) throws SQLException {
        List<String> tables = super.listTables(catalog);
        for (String t : tables) {
            if (t.equalsIgnoreCase(table)) {
                table = t;
                break;
            }
        }
        return table;
    }
}
