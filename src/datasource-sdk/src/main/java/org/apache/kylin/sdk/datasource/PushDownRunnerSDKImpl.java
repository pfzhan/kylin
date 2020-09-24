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
package org.apache.kylin.sdk.datasource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.adhocquery.IPushDownRunner;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PushDownRunnerSDKImpl implements IPushDownRunner {
    private JdbcConnector dataSource;

    @Override
    public void init(KylinConfig config) {
        dataSource = SourceConnectorFactory.getJdbcConnector(config);
    }

    @Override
    public void executeQuery(String query, List<List<String>> returnRows, List<SelectedColumnMeta> returnColumnMeta,
            String project) throws Exception {
        query = dataSource.convertSql(query);

        //extract column metadata
        ResultSet rs = null;
        ResultSetMetaData metaData;
        int columnCount;
        try (Connection conn = dataSource.getConnection(); Statement state = conn.createStatement()) {
            rs = state.executeQuery(query);

            extractResults(rs, returnRows);
            metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();

            // fill in selected column meta
            for (int i = 1; i <= columnCount; ++i) {
                int kylinTypeId = dataSource.toKylinTypeId(metaData.getColumnTypeName(i), metaData.getColumnType(i));
                String kylinTypeName = dataSource.toKylinTypeName(kylinTypeId);
                returnColumnMeta.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i),
                        false, metaData.isCurrency(i), metaData.isNullable(i), false, metaData.getColumnDisplaySize(i),
                        metaData.getColumnLabel(i), metaData.getColumnName(i), null, null, null,
                        metaData.getPrecision(i), metaData.getScale(i), kylinTypeId, kylinTypeName,
                        metaData.isReadOnly(i), false, false));
            }
        } finally {
            DBUtils.closeQuietly(rs);
        }

    }

    @Override
    public void executeUpdate(String sql, String project) throws Exception {
        dataSource.executeUpdate(sql);
    }

    @Override
    public String getName() {
        return QueryContext.PUSHDOWN_RDBMS;
    }

    private void extractResults(ResultSet resultSet, List<List<String>> results) throws SQLException {
        List<String> oneRow = new LinkedList<>();

        while (resultSet.next()) {
            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                oneRow.add((resultSet.getString(i + 1)));
            }

            results.add(new LinkedList<>(oneRow));
            oneRow.clear();
        }
    }
}
