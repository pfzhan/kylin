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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.enumerator.LookupTableEnumerator;
import org.apache.kylin.query.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.modeling.smart.cube.SqlResult;

public class MockupQueryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(MockupQueryExecutor.class);

    private static ThreadLocal<QueryRecord> CURRENT_RECORD = new ThreadLocal<>();

    protected static QueryRecord getCurrentRecord() {
        QueryRecord record = CURRENT_RECORD.get();
        if (record == null) {
            record = new QueryRecord();
            CURRENT_RECORD.set(record);
        }
        return record;
    }

    private static void clearCurrentRecord() {
        CURRENT_RECORD.remove();
    }

    public QueryRecord execute(String projectName, String sql) {
        QueryRecord record = getCurrentRecord();

        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;
        SqlResult sqlResult = new SqlResult();
        record.setSqlResult(sqlResult);

        try {
            conn = QueryConnection.getConnection(projectName);

            statement = conn.createStatement();
            sql = QueryUtil.massageSql(sql, projectName, 0, 0, conn.getSchema());
            resultSet = statement.executeQuery(sql);

            sqlResult.setStatus(SqlResult.Status.SUCCESS);
        } catch (Exception e) {
            if (e.getCause() != null
                    && e.getCause() instanceof com.google.common.cache.CacheLoader.InvalidCacheLoadException) {
                StackTraceElement[] stackTrace = e.getCause().getStackTrace();
                for (StackTraceElement s : stackTrace) {
                    if (s.toString().contains(LookupTableEnumerator.class.getName())) {
                        logger.debug("This query hits table snapshot.");

                        sqlResult.setStatus(SqlResult.Status.SUCCESS);
                        return record;
                    }
                }
            }

            sqlResult.setStatus(SqlResult.Status.FAILED);
            sqlResult.setMessage(e.getMessage());

        } finally {
            DBUtils.closeQuietly(statement);
            DBUtils.closeQuietly(resultSet);
            DBUtils.closeQuietly(conn);
            clearCurrentRecord();
        }

        return record;
    }
}
