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
package org.apache.kylin.sdk.datasource.framework;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.sdk.datasource.adaptor.AdaptorConfig;
import org.apache.kylin.sdk.datasource.adaptor.DefaultAdaptor;
import org.apache.kylin.sdk.datasource.adaptor.MysqlAdaptor;

public class SourceConnectorFactory {
    private SourceConnectorFactory() {
    }

    public static JdbcConnector getJdbcConnector(KylinConfig config) {
        String jdbcUrl = config.getJdbcConnectionUrl();
        String jdbcDriver = config.getJdbcDriver();
        String jdbcUser = config.getJdbcUser();
        String jdbcPass = config.getJdbcPass();
        String adaptorClazz = config.getJdbcAdaptorClass();

        AdaptorConfig jdbcConf = new AdaptorConfig(jdbcUrl, jdbcDriver, jdbcUser, jdbcPass);
        jdbcConf.poolMaxIdle = config.getPoolMaxIdle();
        jdbcConf.poolMinIdle = config.getPoolMinIdle();
        jdbcConf.poolMaxTotal = config.getPoolMaxTotal();
        jdbcConf.datasourceId = config.getJdbcDialect();

        jdbcConf.setConnectRetryTimes(config.getJdbcConnectRetryTimes());
        jdbcConf.setSleepMillisecBetweenRetry(config.getJdbcSleepIntervalBetweenRetry());

        if (adaptorClazz == null)
            adaptorClazz = decideAdaptorClassName(jdbcConf.datasourceId);

        try {
            return new JdbcConnector(AdaptorFactory.createJdbcAdaptor(adaptorClazz, jdbcConf));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get JdbcConnector from env.", e);
        }
    }

    private static String decideAdaptorClassName(String dataSourceId) {
        switch (dataSourceId) {
        case "mysql":
            return MysqlAdaptor.class.getName();
        default:
            return DefaultAdaptor.class.getName();
        }
    }
}
