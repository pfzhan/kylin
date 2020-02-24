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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.kylin.common.StorageURL;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.metadata.PersistException;
import io.kyligence.kap.common.util.EncryptUtil;
import lombok.val;

public class JdbcUtil implements IKeep {

    public static <T> T withTransaction(DataSourceTransactionManager transactionManager, Callback<T> consumer) {
        val definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
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
        val resultSet = conn.getMetaData().getTables(null, null, table, null);
        return resultSet.next();
    }

    public static Properties datasourceParameters(StorageURL url) {
        val props = new Properties();
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

    public interface Callback<T> {
        T handle() throws Exception;

        default void onError() {
            // do nothing by default
        }
    }
}
