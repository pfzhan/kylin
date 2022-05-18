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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryHistoryMapper;
import io.kyligence.kap.metadata.query.QueryHistoryRealizationMapper;
import io.kyligence.kap.metadata.query.QueryStatisticsMapper;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHisStoreUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private static final String CREATE_QUERY_HISTORY_TABLE = "create.queryhistory.store.table";
    private static final String CREATE_QUERY_HISTORY_INDEX1 = "create.queryhistory.store.tableindex1";
    private static final String CREATE_QUERY_HISTORY_INDEX2 = "create.queryhistory.store.tableindex2";
    private static final String CREATE_QUERY_HISTORY_INDEX3 = "create.queryhistory.store.tableindex3";
    private static final String CREATE_QUERY_HISTORY_INDEX4 = "create.queryhistory.store.tableindex4";
    private static final String CREATE_QUERY_HISTORY_INDEX5 = "create.queryhistory.store.tableindex5";

    private static final String CREATE_QUERY_HISTORY_REALIZATION_TABLE = "create.queryhistoryrealization.store.table";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX1 = "create.queryhistoryrealization.store.tableindex1";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX2 = "create.queryhistoryrealization.store.tableindex2";

    private QueryHisStoreUtil() {
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String qhTableName,
            String qhRealizationTableName) {
        return Singletons.getInstance("query-history-sql-session-factory", SqlSessionFactory.class, clz -> {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("query history", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(QueryHistoryMapper.class);
            configuration.addMapper(QueryHistoryRealizationMapper.class);
            configuration.addMapper(QueryStatisticsMapper.class);
            createQueryHistoryIfNotExist((BasicDataSource) dataSource, qhTableName);
            createQueryHistoryRealizationIfNotExist((BasicDataSource) dataSource, qhRealizationTableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createQueryHistoryIfNotExist(BasicDataSource dataSource, String qhTableName)
            throws SQLException, IOException {
        if(JdbcTableUtil.isTableExist(dataSource, qhTableName)) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_TABLE), qhTableName)
                            .getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX1), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX2), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX3), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX4), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX5), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }

    private static void createQueryHistoryRealizationIfNotExist(BasicDataSource dataSource,
            String qhRealizationTableName) throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, qhRealizationTableName)) {
                return;
            }
        } catch (Exception e) {
            log.error("Fail to know if table {} exists", qhRealizationTableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_TABLE),
                            qhRealizationTableName).getBytes(Charset.defaultCharset())),
                    Charset.defaultCharset()));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_INDEX1),
                            qhRealizationTableName, qhRealizationTableName).getBytes(Charset.defaultCharset())),
                    Charset.defaultCharset()));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_INDEX2),
                            qhRealizationTableName, qhRealizationTableName).getBytes(Charset.defaultCharset())),
                    Charset.defaultCharset()));
        }
    }

    public static void cleanQueryHistory() {
        try (SetThreadName ignored = new SetThreadName("QueryHistoryCleanWorker")) {
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            getQueryHistoryDao().deleteQueryHistoriesIfMaxSizeReached();
            getQueryHistoryDao().deleteQueryHistoriesIfRetainTimeReached();
            for (ProjectInstance project : projectManager.listAllProjects()) {
                if (!EpochManager.getInstance().checkEpochOwner(project.getName()))
                    continue;
                try {
                    long startTime = System.currentTimeMillis();
                    log.info("Start to delete query histories that are beyond max size for project<{}>",
                            project.getName());
                    getQueryHistoryDao().deleteQueryHistoriesIfProjectMaxSizeReached(project.getName());
                    log.info("Query histories cleanup for project<{}> finished, it took {}ms", project.getName(),
                            System.currentTimeMillis() - startTime);
                } catch (Exception e) {
                    log.error("clean query histories<" + project.getName() + "> failed", e);
                }
            }
        }
    }

    private static QueryHistoryDAO getQueryHistoryDao() {
        return RDBMSQueryHistoryDAO.getInstance();
    }
}
