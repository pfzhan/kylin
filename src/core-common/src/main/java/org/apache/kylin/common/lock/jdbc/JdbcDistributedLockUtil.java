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

package org.apache.kylin.common.lock.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.StorageURL;

@Slf4j
public class JdbcDistributedLockUtil {
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String CREATE_TABLE_TEMPLATE = "create.jdbc.distributed.lock.table";

    private JdbcDistributedLockUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static void createDistributedLockTableIfNotExist() throws Exception {
        String prefix = getGlobalDictLockTablePrefix();
        String lockTableName = prefix + "LOCK";
        DataSource dataSource = getDataSource();
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, lockTableName)) {
                return;
            }
        } catch (Exception e) {
            log.error("Fail to know if table {} exists", lockTableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties((BasicDataSource) dataSource);
            String sql = String.format(Locale.ROOT, properties.getProperty(CREATE_TABLE_TEMPLATE), prefix, prefix);
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(
                    new InputStreamReader(new ByteArrayInputStream(sql.getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static String getGlobalDictLockTablePrefix() {
        StorageURL url = KylinConfig.getInstanceFromEnv().getJDBCDistributedLockURL();
        return StorageURL.replaceUrl(url);
    }

    public static DataSource getDataSource() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val url = config.getJDBCDistributedLockURL();
        val props = JdbcUtil.datasourceParameters(url);
        return JdbcDataSource.getDataSource(props);
    }
}
