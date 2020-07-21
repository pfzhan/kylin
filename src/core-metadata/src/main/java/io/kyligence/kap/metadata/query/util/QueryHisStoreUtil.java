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

package io.kyligence.kap.metadata.query.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.type.JdbcType;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.query.QueryHistoryMapper;
import io.kyligence.kap.metadata.query.QueryHistoryRealizationMapper;
import io.kyligence.kap.metadata.query.QueryStatisticsMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHisStoreUtil {

    private static final String CREATE_QUERY_HISTORY_TABLE = "create.queryhistory.store.table";
    private static final String CREATE_QUERY_HISTORY_INDEX1 = "create.queryhistory.store.tableindex1";
    private static final String CREATE_QUERY_HISTORY_INDEX2 = "create.queryhistory.store.tableindex2";
    private static final String CREATE_QUERY_HISTORY_INDEX3 = "create.queryhistory.store.tableindex3";
    private static final String CREATE_QUERY_HISTORY_INDEX4 = "create.queryhistory.store.tableindex4";
    private static final String CREATE_QUERY_HISTORY_INDEX5 = "create.queryhistory.store.tableindex5";

    private static final String CREATE_QUERY_HISTORY_REALIZATION_TABLE = "create.queryhistoryrealization.store.table";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX1 = "create.queryhistoryrealization.store.tableindex1";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX2 = "create.queryhistoryrealization.store.tableindex2";

    private volatile static SqlSessionFactory sqlSessionFactory = null;

    private static final Logger logger = LoggerFactory.getLogger(QueryHisStoreUtil.class);

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String qhTableName,
            String qhRealizationTableName) throws SQLException, IOException {
        if (sqlSessionFactory == null) {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("query history", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(QueryHistoryMapper.class);
            configuration.addMapper(QueryHistoryRealizationMapper.class);
            configuration.addMapper(QueryStatisticsMapper.class);
            synchronized (QueryHisStoreUtil.class) {
                if (sqlSessionFactory == null) {
                    createQueryHistoryIfNotExist((BasicDataSource) dataSource, qhTableName);
                    createQueryHistoryRealizationIfNotExist((BasicDataSource) dataSource, qhRealizationTableName);
                    sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
                }
            }
        }
        return sqlSessionFactory;
    }

    private static void createQueryHistoryIfNotExist(BasicDataSource dataSource, String qhTableName)
            throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, qhTableName)) {
                return;
            }
        } catch (Exception e) {
            logger.error("Fail to know if table {} exists", qhTableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_TABLE), qhTableName).getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_INDEX1), qhTableName, qhTableName)
                            .getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_INDEX2), qhTableName, qhTableName)
                            .getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_INDEX3), qhTableName, qhTableName)
                            .getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_INDEX4), qhTableName, qhTableName)
                            .getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_INDEX5), qhTableName, qhTableName)
                            .getBytes())));
        }
    }

    private static void createQueryHistoryRealizationIfNotExist(BasicDataSource dataSource,
            String qhRealizationTableName) throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, qhRealizationTableName)) {
                return;
            }
        } catch (Exception e) {
            logger.error("Fail to know if table {} exists", qhRealizationTableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(String
                    .format(properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_TABLE), qhRealizationTableName)
                    .getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_INDEX1),
                            qhRealizationTableName, qhRealizationTableName).getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_INDEX2),
                            qhRealizationTableName, qhRealizationTableName).getBytes())));
        }
    }
}
