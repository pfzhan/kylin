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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.boot.autoconfigure.session.StoreType;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;
import org.springframework.session.web.context.AbstractHttpSessionApplicationInitializer;
import org.springframework.web.client.RestTemplate;

import com.netflix.loadbalancer.Server;
import com.netflix.loadbalancer.ServerListFilter;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Profile("!dev")
public class HAConfiguration extends AbstractHttpSessionApplicationInitializer {

    @Autowired
    DataSource dataSource;

    @Autowired
    SessionProperties sessionProperties;

    @PostConstruct
    public void initSessionTables() throws Exception {
        if (sessionProperties.getStoreType() != StoreType.JDBC) {
            return;
        }
        val tableName = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix() + "_session";
        if (isTableExists(dataSource.getConnection(), tableName)) {
            log.info("Session table {} already exists", tableName);
            return;
        }
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();

        String sqlFile = "script/schema-session-pg.sql";
        if (dataSource instanceof org.apache.tomcat.jdbc.pool.DataSource
                && ((org.apache.tomcat.jdbc.pool.DataSource) dataSource).getPoolProperties().getDriverClassName()
                        .equals("com.mysql.jdbc.Driver")) {
            sqlFile = "script/schema-session-mysql.sql";
        }
        var sessionScript = IOUtils.toString(getClass().getClassLoader().getResourceAsStream(sqlFile));
        sessionScript = sessionScript.replaceAll("SPRING_SESSION", tableName);
        populator.addScript(new InMemoryResource(sessionScript));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @Bean
    @LoadBalanced
    public RestTemplate restTemplate() {
        val restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        return restTemplate;
    }

    @Bean
    public ServerListFilter<Server> serverListFilter() {
        return servers -> servers;
    }
}
