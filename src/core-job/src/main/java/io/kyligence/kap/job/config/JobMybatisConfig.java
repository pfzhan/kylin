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

package io.kyligence.kap.job.config;

import lombok.var;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;

@Component
@ConditionalOnProperty("spring.job-datasource.url")
public class JobMybatisConfig implements InitializingBean {

    @Autowired
    @Qualifier("jobDataSource")
    private DataSource dataSource;

    public static String JOB_INFO_TABLE = null;
    public static String JOB_LOCK_TABLE = null;


    @Override
    public void afterPropertiesSet() throws Exception {
        String keIdentified = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
        JOB_INFO_TABLE = keIdentified + "_job_info";
        JOB_LOCK_TABLE = keIdentified + "_job_lock";

        String jobInfoFile = "script/schema_job_info_mysql.sql";
        String jobLockFile = "script/schema_job_lock_mysql.sql";
        // todo: add pg sql

        if (!isTableExists(dataSource.getConnection(), JOB_INFO_TABLE)) {
            createTableIfNotExist(keIdentified, jobInfoFile);
        }

        if (!isTableExists(dataSource.getConnection(), JOB_LOCK_TABLE)) {
            createTableIfNotExist(keIdentified, jobLockFile);
        }

    }

    private void createTableIfNotExist(String keIdentified, String sql) throws IOException {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();

        var sessionScript = IOUtils.toString(getClass().getClassLoader().getResourceAsStream(sql), StandardCharsets.UTF_8);
        sessionScript = sessionScript.replaceAll("KE_IDENTIFIED", keIdentified);
        populator.addScript(new InMemoryResource(sessionScript));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, dataSource);
    }
}
