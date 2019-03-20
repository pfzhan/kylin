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

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;
import org.springframework.session.web.context.AbstractHttpSessionApplicationInitializer;
import org.springframework.web.client.RestTemplate;

import com.netflix.loadbalancer.Server;
import com.netflix.loadbalancer.ServerListFilter;

import lombok.val;
import lombok.var;

@Configuration
public class HAConfiguration extends AbstractHttpSessionApplicationInitializer {

    @Autowired
    DataSource dataSource;

    @PostConstruct
    public void initSessionTables() throws IOException {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        var sessionScript = IOUtils
                .toString(getClass().getClassLoader().getResourceAsStream("script/schema-session.sql"));
        val tableName = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix() + "_session";
        sessionScript = sessionScript.replaceAll("SPRING_SESSION", tableName);
        populator.addScript(new InMemoryResource(sessionScript));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @Bean
    @LoadBalanced
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public ServerListFilter<Server> serverListFilter() {
        return servers -> servers;
    }
}
