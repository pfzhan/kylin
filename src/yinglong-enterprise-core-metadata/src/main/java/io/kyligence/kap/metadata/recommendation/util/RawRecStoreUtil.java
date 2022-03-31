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

package io.kyligence.kap.metadata.recommendation.util;

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
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.Singletons;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItemMapper;
import io.kyligence.kap.metadata.transaction.SpringManagedTransactionFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawRecStoreUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String CREATE_REC_TABLE = "create.rawrecommendation.store.table";
    public static final String CREATE_INDEX = "create.rawrecommendation.store.index";

    private RawRecStoreUtil() {
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String tableName) {
        return Singletons.getInstance("raw-recommendation-sql-session-factory", SqlSessionFactory.class, clz -> {
            log.info("Start to build SqlSessionFactory");
            TransactionFactory transactionFactory = new SpringManagedTransactionFactory();
            Environment environment = new Environment("raw recommendation", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(RawRecItemMapper.class);
            createTableIfNotExist((BasicDataSource) dataSource, tableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createTableIfNotExist(BasicDataSource dataSource, String tableName)
            throws IOException, SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            log.info("{} already existed in database", tableName);
            return;
        }

        Properties properties = JdbcUtil.getProperties(dataSource);
        String createTableStmt = String.format(Locale.ROOT, properties.getProperty(CREATE_REC_TABLE), tableName);
        String crateIndexStmt = String.format(Locale.ROOT, properties.getProperty(CREATE_INDEX), tableName, tableName);
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            log.debug("start to create table({})", tableName);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            log.debug("create table finished");
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(crateIndexStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }

        if (!JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            log.debug("failed to create table({})", tableName);
            throw new IllegalStateException(String.format(Locale.ROOT, "create table(%s) failed", tableName));
        } else {
            log.debug("table({}) already exists.", tableName);
        }
    }
}
