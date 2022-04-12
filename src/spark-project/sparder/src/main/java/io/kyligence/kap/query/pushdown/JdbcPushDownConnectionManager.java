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

 


package io.kyligence.kap.query.pushdown;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.kylin.common.KylinConfig;

public class JdbcPushDownConnectionManager {

    private volatile static JdbcPushDownConnectionManager manager = null;

    static JdbcPushDownConnectionManager getConnectionManager(KylinConfig config) throws ClassNotFoundException {
        if (manager == null) {
            synchronized (JdbcPushDownConnectionManager.class) {
                if (manager == null) {
                    manager = new JdbcPushDownConnectionManager(config);
                }
            }
        }
        return manager;
    }

    private final BasicDataSource dataSource;

    private JdbcPushDownConnectionManager(KylinConfig config) throws ClassNotFoundException {
        dataSource = new BasicDataSource();

        Class.forName(config.getJdbcDriverClass());
        dataSource.setDriverClassName(config.getJdbcDriverClass());
        dataSource.setUrl(config.getJdbcUrl());
        dataSource.setUsername(config.getJdbcUsername());
        dataSource.setPassword(config.getJdbcPassword());
        dataSource.setMaxActive(config.getPoolMaxTotal());
        dataSource.setMaxIdle(config.getPoolMaxIdle());
        dataSource.setMinIdle(config.getPoolMinIdle());

        // Default settings
        dataSource.setTestOnBorrow(true);
        dataSource.setValidationQuery("select 1");
        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(300);
    }

    public void close() {
        try {
            dataSource.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close(Connection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
