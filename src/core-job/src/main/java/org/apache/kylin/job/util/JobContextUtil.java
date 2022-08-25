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

package org.apache.kylin.job.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.session.SqlSessionManager;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.SpringContext;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.config.JobTableInterceptor;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.mapper.JobLockMapper;
import org.apache.kylin.metadata.transaction.SpringManagedTransactionFactory;
import org.apache.kylin.rest.delegate.ModelMetadataBaseInvoker;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobContextUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private static final String CREATE_JOB_INFO_TABLE = "create.job.info.table";

    private static final String CREATE_JOB_LOCK_TABLE = "create.job.lock.table";

    private static JobInfoMapper jobInfoMapper;

    private static JobLockMapper jobLockMapper;

    private static JobInfoDao jobInfoDao;

    private static JobContext jobContext;

    private static SqlSessionManager sqlSessionManager;

    private static DataSourceTransactionManager transactionManager;

    private static JobTableInterceptor jobTableInterceptor = new JobTableInterceptor();

    synchronized public static JobInfoDao getJobInfoDaoForTest(KylinConfig config) {
        initMappers(config);
        if (null == jobInfoDao) {
            jobInfoDao = new JobInfoDao();
            jobInfoDao.setJobInfoMapper(jobInfoMapper);
            jobInfoDao.setModelMetadataInvoker(ModelMetadataBaseInvoker.getInstance());
        }
        return jobInfoDao;
    }

    synchronized public static JobContext getJobContextForTest(KylinConfig config) {
        initMappers(config);
        if (null == jobContext) {
            config.setProperty("kylin.job.scheduler.master-poll-interval-second", "1");
            config.setProperty("kylin.job.scheduler.poll-interval-second", "1");
            jobContext = new JobContext();
            jobContext.setKylinConfig(config);
            jobContext.setJobInfoMapper(jobInfoMapper);
            jobContext.setJobLockMapper(jobLockMapper);
            jobContext.setTransactionManager(transactionManager);
            jobContext.init();
        }
        return jobContext;
    }

    synchronized private static void initMappers(KylinConfig config) {
        if (null != jobInfoMapper && null != jobLockMapper) {
            return;
        }
        StorageURL url = config.getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = null;
        try {
            dataSource = JdbcDataSource.getDataSource(props);
            transactionManager = new DataSourceTransactionManager(dataSource);
            SqlSessionFactory sqlSessionFactory = getSqlSessionFactory(dataSource);
            sqlSessionManager = SqlSessionManager.newInstance(sqlSessionFactory);
            addPluginForSqlSessionManager(sqlSessionManager);
            jobInfoMapper = sqlSessionManager.getMapper(JobInfoMapper.class);
            jobInfoMapper.deleteAllJob();
            jobLockMapper = sqlSessionManager.getMapper(JobLockMapper.class);
            jobLockMapper.deleteAllJobLock();
        } catch (Exception e) {
            throw new RuntimeException("initialize mybatis mappers failed", e);
        }
    }

    private static void addPluginForSqlSessionManager(SqlSessionManager sqlSessionManager){
        List<Interceptor> interceptors = sqlSessionManager.getConfiguration().getInterceptors();

        if (!interceptors.contains(jobTableInterceptor)){
            sqlSessionManager.getConfiguration().addInterceptor(jobTableInterceptor);
        }

    }


    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource) throws SQLException, IOException {
        log.info("Start to build data loading SqlSessionFactory");
        TransactionFactory transactionFactory = new SpringManagedTransactionFactory();
        Environment environment = new Environment("data loading", transactionFactory, dataSource);
        Configuration configuration = new Configuration(environment);
        configuration.setUseGeneratedKeys(true);
        configuration.setJdbcTypeForNull(JdbcType.NULL);
        configuration.addMapper(JobInfoMapper.class);
        configuration.addMapper(JobLockMapper.class);
        // configuration.setLogImpl(StdOutImpl.class);
        configuration.setCacheEnabled(false);
        setMapperXML(configuration);
        createTableIfNotExist((BasicDataSource) dataSource, "job_info");
        createTableIfNotExist((BasicDataSource) dataSource, "job_lock");
        return new SqlSessionFactoryBuilder().build(configuration);
    }

    private static void setMapperXML(Configuration configuration) throws IOException {
        ResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resourceResolver.getResources("classpath:/mybatis-mapper/*Mapper.xml");
        for (Resource resource : resources) {
            XMLMapperBuilder xmlMapperBuilder = new XMLMapperBuilder(resource.getInputStream(),
                    configuration, resource.toString(), configuration.getSqlFragments());
            xmlMapperBuilder.parse();
        }
    }

    private static void createTableIfNotExist(BasicDataSource dataSource, String tableName)
            throws IOException, SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            log.info("{} already existed in database", tableName);
            return;
        }

        String createTableStmtProp = "";
        if ("job_info".equals(tableName)) {
            createTableStmtProp = CREATE_JOB_INFO_TABLE;
        } else {
            createTableStmtProp = CREATE_JOB_LOCK_TABLE;
        }

        Properties properties = JdbcUtil.getProperties(dataSource);
        String createTableStmt = String.format(Locale.ROOT, properties.getProperty(createTableStmtProp), tableName);
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            log.debug("start to create table({})", tableName);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            log.debug("create table finished");
        }

        if (!JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            log.debug("failed to create table({})", tableName);
            throw new IllegalStateException(String.format(Locale.ROOT, "create table(%s) failed", tableName));
        } else {
            log.debug("table({}) already exists.", tableName);
        }
    }

    public static JobInfoDao getJobInfoDao(KylinConfig config) {
        if (config.isUTEnv()) {
            return getJobInfoDaoForTest(config);
        } else {
            return SpringContext.getBean(JobInfoDao.class);
        }
    }

    public static JobContext getJobContext(KylinConfig config) {
        if (config.isUTEnv()) {
            return getJobContextForTest(config);
        } else {
            return SpringContext.getBean(JobContext.class);
        }
    }

    public static DataSourceTransactionManager getTransactionManager(KylinConfig config) {
        if (config.isUTEnv()) {
            synchronized (JobContextUtil.class) {
                initMappers(config);
                return transactionManager;
            }
        } else {
            return SpringContext.getBean(DataSourceTransactionManager.class);
        }
    }

    // for test only
    synchronized public static void cleanUp() {
        try {
            if (null != jobContext) {
                jobContext.destroy();
            }
            jobInfoMapper = null;
            jobLockMapper = null;
            jobInfoDao = null;
            jobContext = null;
            sqlSessionManager = null;
            transactionManager = null;
        } catch (Exception e) {
            log.error("JobContextUtil clean up failed.");
            throw new RuntimeException("JobContextUtil clean up failed.", e);
        }

    }
}
