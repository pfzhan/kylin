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

package io.kyligence.kap.metadata.state;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.query.util.JdbcTableUtil;
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
public class ShareStateUtil {
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private static final String CREATE_SHARE_STATE_TABLE = "create.sharestate.store.table";
    private static final String CREATE_SHARE_STATE_INDEX1 = "create.sharestate.store.tableindex1";

    private ShareStateUtil() {

    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String tableName) {
        return Singletons.getInstance("share-state-sql-session-factory", SqlSessionFactory.class, clz -> {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            org.apache.ibatis.mapping.Environment environment
                    = new Environment("share state", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(ShareStateMapper.class);
            createShareStateIfNotExist((BasicDataSource) dataSource, tableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createShareStateIfNotExist(BasicDataSource dataSource, String tableName)
            throws SQLException, IOException {
        if(JdbcTableUtil.isTableExist(dataSource, tableName)) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_SHARE_STATE_TABLE), tableName)
                            .getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_SHARE_STATE_INDEX1), tableName,
                            tableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }

}
