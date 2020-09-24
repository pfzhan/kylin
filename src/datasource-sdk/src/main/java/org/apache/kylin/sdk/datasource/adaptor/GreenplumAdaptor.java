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

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.FilteredRowSet;
import javax.sql.rowset.Predicate;

import com.sun.rowset.FilteredRowSetImpl;

public class GreenplumAdaptor extends DefaultAdaptor {

    public GreenplumAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    @Override
    public CachedRowSet getTableColumns(final String schema, final String table) throws SQLException {
        try (Connection conn = getConnection();
                ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), null, null, null)) {
            FilteredRowSet r = new FilteredRowSetImpl();
            r.setFilter(new FilterPredicate(schema, table));
            r.populate(rs);
            return r;
        }
    }

    @Override
    public CachedRowSet getTable(String schema, String table) throws SQLException {
        try (Connection conn = getConnection();
                ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), null, null, null)) {
            FilteredRowSet r = new FilteredRowSetImpl();
            r.setFilter(new FilterPredicate(schema, table));
            r.populate(rs);
            return r;
        }
    }

    private static class FilterPredicate implements Predicate {
        final String schema;
        final String table;

        FilterPredicate(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        @Override
        public boolean evaluate(RowSet rs) {
            try {
                String jSchema = rs.getString("TABLE_SCHEM");
                String jName = rs.getString("TABLE_NAME");

                return jSchema.equalsIgnoreCase(schema) && jName.equalsIgnoreCase(table);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean evaluate(Object value, int column) throws SQLException {
            return false;
        }

        @Override
        public boolean evaluate(Object value, String columnName) throws SQLException {
            return false;
        }
    }
}
