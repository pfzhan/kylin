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

package io.kyligence.kap.metadata.favorite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Getter
public class JDBCManager {

    private static final Logger logger = LoggerFactory.getLogger(JDBCManager.class);

    private TransactionTemplate transactionTemplate;
    private JdbcTemplate jdbcTemplate;
    private DataSource dataSource;
    private DataSourceTransactionManager dataSourceTransactionManager;

    public static JDBCManager getInstance(KylinConfig config) {
        return config.getManager(JDBCManager.class);
    }

    // called by reflection
    static JDBCManager newInstance(KylinConfig config) throws IOException {
        return new JDBCManager(config);
    }

    private JDBCManager(KylinConfig config) {
        try {
            dataSource = BasicDataSourceFactory.createDataSource(initDbcpProps(config));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private Properties initDbcpProps(KylinConfig kylinConfig) {
        StorageURL url = kylinConfig.getFavoriteStorageUrl();
        Map<String, String> props = Maps.newHashMap(url.getAllParameters());
        List<String> mandatoryItems = Arrays.asList("url", "username", "password");

        for (String item : mandatoryItems) {
            Preconditions.checkNotNull(props.get(item),
                    "Setting item \"" + item + "\" is mandatory for Jdbc connections.");
        }

        logger.info("Connecting to Jdbc with url:" + props.get("url") + " by user " + props.get("username"));

        Properties ret = new Properties();
        ret.putAll(props);

        putIfMissing(ret, "driverClassName", "com.mysql.jdbc.Driver");
        putIfMissing(ret, "maxActive", "100");
        putIfMissing(ret, "maxIdle", "100");
        putIfMissing(ret, "maxWait", "1000");
        putIfMissing(ret, "removeAbandoned", "true");
        putIfMissing(ret, "removeAbandonedTimeout", "180");
        putIfMissing(ret, "testOnBorrow", "true");
        putIfMissing(ret, "testWhileIdle", "true");
        putIfMissing(ret, "validationQuery", "select 1");
        return ret;
    }

    private void putIfMissing(Properties map, String key, String value) {
        if (map.containsKey(key) == false)
            map.put(key, value);
    }

}
