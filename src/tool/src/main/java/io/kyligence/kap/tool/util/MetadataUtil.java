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
package io.kyligence.kap.tool.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private MetadataUtil() {
    }

    public static String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
        }
        return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");
    }

    public static DataSource getDataSource(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);

        return JdbcDataSource.getDataSource(props);
    }

    public static void createTableIfNotExist(BasicDataSource dataSource, String tableName, String tableSql,
            List<String> indexSqlList) throws IOException, SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            return;
        }

        if (null == indexSqlList) {
            indexSqlList = Lists.newArrayList();
        }

        Properties properties = JdbcUtil.getProperties(dataSource);
        String createTableStmt = String.format(Locale.ROOT, properties.getProperty(tableSql), tableName);
        List<String> crateIndexStmtList = indexSqlList.stream()
                .map(indexSql -> String.format(Locale.ROOT, properties.getProperty(indexSql), tableName, tableName))
                .collect(Collectors.toList());
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.setStopOnError(true);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            crateIndexStmtList.forEach(crateIndexStmt -> sr.runScript(new InputStreamReader(
                    new ByteArrayInputStream(crateIndexStmt.getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET)));

        }
    }

    public static void createTableIfNotExist(BasicDataSource dataSource, String tableName, String createTableStmt)
            throws SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            return;
        }

        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.setStopOnError(true);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }
}
