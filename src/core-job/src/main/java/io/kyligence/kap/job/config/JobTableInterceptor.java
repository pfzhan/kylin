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

import java.lang.reflect.Method;

import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.domain.JobLock;
import io.kyligence.kap.job.rest.JobMapperFilter;

@ConditionalOnProperty("spring.job-datasource.url")
@Component
@Intercepts({
        @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class }),
        @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class }),
        @Signature(type = Executor.class, method = "queryCursor", args = { MappedStatement.class, Object.class,
                RowBounds.class }),
        @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }) })
public class JobTableInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(JobTableInterceptor.class);

    @Override
    public Object intercept(Invocation invocation) throws Throwable {

        Object target = invocation.getTarget();
        Method method = invocation.getMethod();
        Object[] args = invocation.getArgs();

        if (JobMybatisConfig.JOB_INFO_TABLE == null || JobMybatisConfig.JOB_LOCK_TABLE == null) {
            logger.info("mybatis table not init, skip");
            return null;
        }

        if (args[1] == null) {
            MapperMethod.ParamMap map = new MapperMethod.ParamMap();
            map.put("jobLockTable", JobMybatisConfig.JOB_LOCK_TABLE);
            map.put("jobInfoTable", JobMybatisConfig.JOB_INFO_TABLE);
            invocation.getArgs()[1] = map;
        } else if (args[1].getClass() == MapperMethod.ParamMap.class) {
            MapperMethod.ParamMap map = (MapperMethod.ParamMap) args[1];
            map.put("jobLockTable", JobMybatisConfig.JOB_LOCK_TABLE);
            map.put("jobInfoTable", JobMybatisConfig.JOB_INFO_TABLE);
        } else if (args[1].getClass() == JobMapperFilter.class) {
            JobMapperFilter mapperFilter = (JobMapperFilter) args[1];
            mapperFilter.setJobInfoTable(JobMybatisConfig.JOB_INFO_TABLE);
        } else if (args[1].getClass() == JobInfo.class) {
            JobInfo jobInfo = (JobInfo) args[1];
            jobInfo.setJobInfoTable(JobMybatisConfig.JOB_INFO_TABLE);
        } else if (args[1].getClass() == JobLock.class) {
            JobLock jobLock = (JobLock) args[1];
            jobLock.setJobLockTable(JobMybatisConfig.JOB_LOCK_TABLE);
        } else {
            logger.error("miss type of param {}", args[1].getClass());
        }

        return invocation.proceed();
    }
}
