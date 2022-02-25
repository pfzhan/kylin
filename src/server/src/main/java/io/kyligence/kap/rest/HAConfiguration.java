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
package io.kyligence.kap.rest;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isColumnExists;
import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kylin.common.KylinConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.boot.autoconfigure.session.StoreType;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;
import org.springframework.session.web.context.AbstractHttpSessionApplicationInitializer;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestContextListener;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Profile("!dev")
public class HAConfiguration extends AbstractHttpSessionApplicationInitializer {

    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS %s";

    @Autowired
    DataSource dataSource;

    @Autowired
    SessionProperties sessionProperties;

    @VisibleForTesting
    void dropSessionTable(JdbcTemplate jdbcTemplate, String tableName) throws IOException {
        log.info("Drop session table {} ", tableName);
        jdbcTemplate.execute(String.format(DROP_TABLE_SQL, tableName));
    }

    @VisibleForTesting
    void initSessionTable(String replaceName, String sqlFile) throws IOException {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();

        var sessionScript = IOUtils.toString(getClass().getClassLoader().getResourceAsStream(sqlFile));
        sessionScript = sessionScript.replaceAll("SPRING_SESSION", replaceName);
        populator.addScript(new InMemoryResource(sessionScript));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @PostConstruct
    public void initSessionTables() throws Exception {
        if (sessionProperties.getStoreType() != StoreType.JDBC) {
            return;
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        String tableName = kylinConfig.getMetadataUrlPrefix() + "_session";
        String attributesTableName = tableName + "_attributes";

        String sessionFile = "script/schema-session-pg.sql";
        String sessionAttributesFile = "script/schema-session-attributes-pg.sql";
        if (dataSource instanceof org.apache.commons.dbcp2.BasicDataSource
                && ((org.apache.commons.dbcp2.BasicDataSource) dataSource).getDriverClassName()
                        .startsWith("com.mysql")) {
            sessionFile = "script/schema-session-mysql.sql";
            sessionAttributesFile = "script/schema-session-attributes-mysql.sql";

            // mysql table name is case sensitive, sql file is using capital letters.
            attributesTableName = tableName + "_ATTRIBUTES";
        }

        boolean tableExists = isTableExists(dataSource.getConnection(), tableName);
        boolean primaryIdExists = isColumnExists(dataSource.getConnection(), tableName, "PRIMARY_ID");

        if (tableExists && !primaryIdExists) {
            if (kylinConfig.isUpdateSessionTableAutomatically()) {
                JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                dropSessionTable(jdbcTemplate, attributesTableName);
                dropSessionTable(jdbcTemplate, tableName);
            } else {
                log.error("Session table schema may be not suitable. "
                        + "Please set kylin.web.session.table.auto-upgrade=true "
                        + "or update session table schema manually.");
            }
        }

        tableExists = isTableExists(dataSource.getConnection(), tableName);
        if (!tableExists) {
            initSessionTable(tableName, sessionFile);
        }

        tableExists = isTableExists(dataSource.getConnection(), attributesTableName);
        if (!tableExists) {
            initSessionTable(tableName, sessionAttributesFile);
        }
    }

    @Bean
    @LoadBalanced
    public RestTemplate restTemplate() {
        val restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory(
                HttpClientBuilder.create().disableCookieManagement().useSystemProperties().build()));
        restTemplate.setUriTemplateHandler(new ProxyUriTemplateHandler());
        return restTemplate;
    }

    @Bean
    public RequestContextListener requestContextListener() {
        return new RequestContextListener();
    }
}
