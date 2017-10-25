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

package io.kyligence.kap.common.persistence;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class JDBCConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(JDBCConnectionManager.class);

    private static JDBCConnectionManager INSTANCE = null;

    private static Object lock = new Object();

    public static JDBCConnectionManager getConnectionManager() {
        if (INSTANCE == null) {
            synchronized (lock) {
                if (INSTANCE == null) {
                    INSTANCE = new JDBCConnectionManager(KylinConfig.getInstanceFromEnv());
                }
            }
        }
        return INSTANCE;
    }

    // ============================================================================

    private final Map<String, String> dbcpProps;
    private final DataSource dataSource;

    private JDBCConnectionManager(KylinConfig config) {
        try {
            this.dbcpProps = initDbcpProps(config);

            dataSource = BasicDataSourceFactory.createDataSource(getDbcpProperties());
            Connection conn = getConn();
            DatabaseMetaData mdm = conn.getMetaData();
            logger.info("Connected to " + mdm.getDatabaseProductName() + " " + mdm.getDatabaseProductVersion());
            closeQuietly(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> initDbcpProps(KylinConfig config) {
        // metadataUrl is like "kylin_default_instance@jdbc,url=jdbc:mysql://localhost:3306/kylin,username=root,password=xxx"
        StorageURL metadataUrl = config.getMetadataUrl();
        JDBCResourceStore.checkScheme(metadataUrl);

        LinkedHashMap<String, String> ret = new LinkedHashMap<>(metadataUrl.getAllParameters());
        List<String> mandatoryItems = Arrays.asList("url", "username", "password");

        for (String item : mandatoryItems) {
            Preconditions.checkNotNull(ret.get(item),
                    "Setting item \"" + item + "\" is mandatory for Jdbc connections.");
        }

        logger.info("Connecting to Jdbc with url:" + ret.get("url") + " by user " + ret.get("username"));

        putIfMissing(ret, "driverClassName", "com.mysql.jdbc.Driver");
        putIfMissing(ret, "maxActive", "5");
        putIfMissing(ret, "maxIdle", "5");
        putIfMissing(ret, "maxWait", "1000");
        putIfMissing(ret, "removeAbandoned", "true");
        putIfMissing(ret, "removeAbandonedTimeout", "180");
        putIfMissing(ret, "testOnBorrow", "true");
        putIfMissing(ret, "testWhileIdle", "true");
        putIfMissing(ret, "validationQuery", "select 1");
        return ret;
    }

    private void putIfMissing(LinkedHashMap<String, String> map, String key, String value) {
        if (map.containsKey(key) == false)
            map.put(key, value);
    }

    public final Connection getConn() throws SQLException {
        return dataSource.getConnection();
    }

    public Properties getDbcpProperties() {
        Properties ret = new Properties();
        ret.putAll(dbcpProps);
        return ret;
    }

    public static void closeQuietly(AutoCloseable obj) {
        if (obj != null) {
            try {
                obj.close();
            } catch (Exception e) {
                logger.warn("Error closing " + obj, e);
            }
        }
    }
}
