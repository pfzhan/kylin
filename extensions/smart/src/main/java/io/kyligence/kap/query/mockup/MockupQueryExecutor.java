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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.query.QueryDataSource;
import org.apache.kylin.query.enumerator.LookupTableEnumerator;
import org.apache.kylin.query.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockupQueryExecutor implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MockupQueryExecutor.class);

    private final KylinConfig kylinConfig;

    private QueryDataSource queryDataSource = new QueryDataSource();

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
            sql = QueryUtil.massageSql(sql, projectName, 0, 0);
            resultSet = statement.executeQuery(sql);
        } catch (Exception e) {
            if (e.getCause() != null
                    && e.getCause() instanceof com.google.common.cache.CacheLoader.InvalidCacheLoadException) {
                StackTraceElement[] stackTrace = e.getCause().getStackTrace();
                for (StackTraceElement s : stackTrace) {
                    if (s.toString().contains(LookupTableEnumerator.class.getName())) {
                        logger.info("Skip dry run because this query only hits lookup tables.");
                        break;
                    }
                }
            }
        } finally {
            DBUtils.closeQuietly(statement);
            DBUtils.closeQuietly(resultSet);
            DBUtils.closeQuietly(conn);
        }
    }

    @Override
    public void close() throws IOException {
        queryDataSource.clearCache();
    }
}
