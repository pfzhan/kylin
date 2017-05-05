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

package io.kyligence.kap.query.mockup;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.query.QueryDataSource;
import org.apache.kylin.query.util.QueryUtil;

public class MockupQueryExecutor implements Closeable {
    private final KylinConfig kylinConfig;

    private QueryDataSource queryDataSource = new QueryDataSource();
    private List<Connection> connections = new ArrayList<>();

    public MockupQueryExecutor(AbstractQueryRecorder<?> queryRecorder) {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        AbstractQueryRecorder.CURRENT.set(queryRecorder);
    }

    public void execute(String projectName, String sql) throws SQLException {
        Connection conn = queryDataSource.get(projectName, kylinConfig).getConnection();

        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = conn.createStatement();
            sql = QueryUtil.massageSql(sql);
            resultSet = statement.executeQuery(sql);
        } finally {
            DBUtils.closeQuietly(statement);
            DBUtils.closeQuietly(resultSet);
        }
    }

    @Override
    public void close() throws IOException {
        for (Connection conn : connections) {
            DBUtils.closeQuietly(conn);
        }
        queryDataSource.clearCache();
    }
}
