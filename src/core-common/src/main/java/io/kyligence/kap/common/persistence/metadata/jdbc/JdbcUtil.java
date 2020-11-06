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
package io.kyligence.kap.common.persistence.metadata.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.msgpack.core.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.metadata.PersistException;
import io.kyligence.kap.common.util.EncryptUtil;
import lombok.val;

public class JdbcUtil implements IKeep {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtil.class);

    public static <T> T withTransaction(DataSourceTransactionManager transactionManager, Callback<T> consumer) {
        return withTransaction(transactionManager, consumer, TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }

    public static <T> T withTransaction(DataSourceTransactionManager transactionManager, Callback<T> consumer,
            int isolationLevel) {
        val definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(isolationLevel);
        val status = transactionManager.getTransaction(definition);
        try {
            T result = consumer.handle();
            transactionManager.commit(status);
            return result;
        } catch (Exception e) {
            transactionManager.rollback(status);
            if (e instanceof DataIntegrityViolationException) {
                consumer.onError();
            }
            throw new PersistException("persist messages failed", e);
        }
    }

    public static boolean isTableExists(Connection conn, String table) throws SQLException {
        return isAnyTableExists(conn, table, table.toUpperCase());
    }

    private static boolean isAnyTableExists(Connection conn, String... tables) throws SQLException {
        try {
            for (String table : tables) {
                val resultSet = conn.getMetaData().getTables(null, null, table, null);
                if (resultSet.next()) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("Fail to know if table {} exists", tables, e);
        } finally {
            if (!conn.isClosed())
                conn.close();
        }
        return false;
    }

    public static Properties datasourceParameters(StorageURL url) {
        return KylinConfig.getInstanceFromEnv().isUTEnv() //
                ? datasourceParametersForUT(url) //
                : datasourceParametersForProd(url);
    }

    public static Properties datasourceParametersForProd(StorageURL url) {
        Properties props = new Properties();
        props.put("driverClassName", "org.postgresql.Driver");
        props.put("url", "jdbc:postgresql://sandbox:5432/kylin");
        props.put("username", "postgres");
        props.put("maxTotal", "50");
        props.putAll(url.getAllParameters());
        String password = props.getProperty("password", "");
        if (EncryptUtil.isEncrypted(password)) {
            password = EncryptUtil.decryptPassInKylin(password);
        }
        props.put("password", password);
        return props;
    }

    public static Properties datasourceParametersForUT(StorageURL url) {
        Properties props = new Properties();
        props.put("driverClassName", "org.h2.Driver");
        props.put("url", "jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MySQL");
        props.put("username", "sa");
        props.put("password", "");
        props.put("maxTotal", "50");
        props.putAll(url.getAllParameters());
        return props;
    }

    public static Properties getProperties(BasicDataSource dataSource) throws IOException {
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

    public interface Callback<T> {
        T handle() throws Exception;

        default void onError() {
            // do nothing by default
        }
    }

    @VisibleForTesting
    public static JdbcTemplate getJdbcTemplate(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
