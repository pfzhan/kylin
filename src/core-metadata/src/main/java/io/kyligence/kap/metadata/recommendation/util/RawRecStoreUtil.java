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
import java.io.InputStream;
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
import org.apache.ibatis.type.JdbcType;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItemMapper;
import io.kyligence.kap.metadata.transaction.SpringManagedTransactionFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawRecStoreUtil {

    private static final String CREATE_REC_TABLE = "create.rawrecommendation.store.table";
    private static final String CREATE_INDEX = "create.rawrecommendation.store.index";
    private static SqlSessionFactory sqlSessionFactory = null;

    private RawRecStoreUtil() {
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String tableName)
            throws IOException, SQLException {
        if (sqlSessionFactory == null) {
            log.info("Start to build SqlSessionFactory");
            TransactionFactory transactionFactory = new SpringManagedTransactionFactory();
            Environment environment = new Environment("raw recommendation", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(RawRecItemMapper.class);
            synchronized (RawRecStoreUtil.class) {
                if (sqlSessionFactory == null) {
                    createTableIfNotExist((BasicDataSource) dataSource, tableName);
                    sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
                }
            }
        }
        return sqlSessionFactory;
    }

    private static void createTableIfNotExist(BasicDataSource dataSource, String tableName)
            throws IOException, SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            log.info("{} already existed in database", tableName);
            return;
        }

        Properties properties = getProperties(dataSource);
        String createTableStmt = String.format(properties.getProperty(CREATE_REC_TABLE), tableName);
        String crateIndexStmt = String.format(properties.getProperty(CREATE_INDEX), tableName, tableName);
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes())));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(crateIndexStmt.getBytes())));
        }
    }

    private static Properties getProperties(BasicDataSource dataSource) throws IOException {
        String fileName;
        switch (dataSource.getDriverClassName()) {
        case "org.postgresql.Driver":
            fileName = "metadata-jdbc-postgresql.properties";
            break;
        case "com.mysql.jdbc.Driver":
            fileName = "metadata-jdbc-mysql.properties";
            break;
        case "org.h2.Driver":
            fileName = "metadata-jdbc-h2.properties";
            break;
        default:
            throw new IllegalArgumentException("Unsupported jdbc driver");
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }

}
