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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import io.kyligence.kap.job.condition.JobModeCondition;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Conditional(JobModeCondition.class)
public class JobMybatisConfig implements InitializingBean {

    @Autowired
    @Qualifier("jobDataSource")
    private DataSource dataSource;

    public static String JOB_INFO_TABLE = "job_info";
    public static String JOB_LOCK_TABLE = "job_lock";

    @Override
    public void afterPropertiesSet() throws Exception {
        StorageURL jobMetadataUrl = KylinConfig.getInstanceFromEnv().getJobMetadataUrl();
        String keIdentified = jobMetadataUrl.getIdentifier();
        if (StringUtils.isEmpty(jobMetadataUrl.getScheme())){
            log.info("metadata from file");
            keIdentified = "file";
        }
        JOB_INFO_TABLE = keIdentified + "_job_info";
        JOB_LOCK_TABLE = keIdentified + "_job_lock";

        String jobInfoFile = "script/schema_job_info_mysql.sql";
        String jobLockFile = "script/schema_job_lock_mysql.sql";
        // todo: add pg sql

        Method[] declaredMethods = ReflectionUtils.getDeclaredMethods(dataSource.getClass());
        List<Method> getDriverClassNameMethodList = Arrays.stream(declaredMethods)
                .filter(method -> "getDriverClassName".equals(method.getName())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(getDriverClassNameMethodList)) {
            log.warn("can not get method of [getDriverClassName] from datasource type = {}", dataSource.getClass());
        } else {
            Method methodOfgetDriverClassName = getDriverClassNameMethodList.get(0);
            String driverClassName = (String) ReflectionUtils.invokeMethod(methodOfgetDriverClassName, dataSource);
            if (driverClassName.startsWith("com.mysql")) {
                // mysql, is default
                log.info("driver class name = {}, is mysql", driverClassName);
            } else if (driverClassName.equals("org.h2.Driver")) {
                log.info("driver class name = {}, is H2 ", driverClassName);
                jobInfoFile = "script/schema_job_info_h2.sql";
                jobLockFile = "script/schema_job_lock_h2.sql";
            } else {
                String errorMsg = String.format("driver class name = %1, should add support", driverClassName);
                log.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }

        if (!isTableExists(dataSource.getConnection(), JOB_INFO_TABLE)) {
            createTableIfNotExist(keIdentified, jobInfoFile);
        }

        if (!isTableExists(dataSource.getConnection(), JOB_LOCK_TABLE)) {
            createTableIfNotExist(keIdentified, jobLockFile);
        }

    }

    private void createTableIfNotExist(String keIdentified, String sql) throws IOException {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();

        var sessionScript = IOUtils.toString(getClass().getClassLoader().getResourceAsStream(sql),
                StandardCharsets.UTF_8);
        sessionScript = sessionScript.replaceAll("KE_IDENTIFIED", keIdentified);
        populator.addScript(new InMemoryResource(sessionScript));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, dataSource);
    }
}
