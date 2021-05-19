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

package io.kyligence.kap.metadata.streaming.util;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.streaming.RDBMSStreamingJobStatsDAO;
import io.kyligence.kap.metadata.streaming.RowCountDetailByTimeMapper;
import io.kyligence.kap.metadata.streaming.StreamingJobStatsMapper;
import io.kyligence.kap.metadata.streaming.StreamingStatisticsMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.Singletons;

import javax.sql.DataSource;
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

@Slf4j
public class StreamingJobStatsStoreUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private static final String CREATE_STREAMING_JOB_STATS_TABLE = "create.streamingjobstats.store.table";
    private static final String CREATE_STREAMING_JOB_STATS_INDEX1 = "create.streamingjobstats.store.tableindex1";
    private static final String CREATE_STREAMING_JOB_STATS_INDEX2 = "create.streamingjobstats.store.tableindex2";

    private StreamingJobStatsStoreUtil() {
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String tableName) {
        return Singletons.getInstance("streaming-job-stats-session-factory", SqlSessionFactory.class, clz -> {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("streaming job stats", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(StreamingJobStatsMapper.class);
            configuration.addMapper(RowCountDetailByTimeMapper.class);
            configuration.addMapper(StreamingStatisticsMapper.class);
            createStreamingJobStatsIfNotExist((BasicDataSource) dataSource, tableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createStreamingJobStatsIfNotExist(BasicDataSource dataSource, String tableName)
            throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, tableName)) {
                return;
            }
        } catch (Exception e) {
            log.error("Fail to know if table {} exists", tableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(Locale.ROOT, properties.getProperty(CREATE_STREAMING_JOB_STATS_TABLE), tableName).getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(Locale.ROOT, properties.getProperty(CREATE_STREAMING_JOB_STATS_INDEX1), tableName, tableName)
                            .getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(Locale.ROOT, properties.getProperty(CREATE_STREAMING_JOB_STATS_INDEX2), tableName, tableName)
                            .getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET));
        }
    }

    public static void cleanStreamingJobStats() {
        String oldThreadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("streamingJobStatsCleanWorker");
            getStreamingJobStatsDao().deleteSJSIfRetainTimeReached();
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }
    }

    private static RDBMSStreamingJobStatsDAO getStreamingJobStatsDao() {
        return RDBMSStreamingJobStatsDAO.getInstance();
    }
}
