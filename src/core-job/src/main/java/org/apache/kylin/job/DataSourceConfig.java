/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.job.condition.JobModeCondition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import lombok.val;

@Configuration
@Conditional(JobModeCondition.class)
public class DataSourceConfig {
    @Bean("jobDataSource")
    @Primary
    public DataSource dataSource() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val url = config.getJobMetadataUrl();
        val props = datasourceParameters(url);
        val datasource = JdbcDataSource.getDataSource(props);
        return datasource;
    }

}
