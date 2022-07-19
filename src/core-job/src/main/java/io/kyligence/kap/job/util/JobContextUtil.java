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

package io.kyligence.kap.job.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.session.SqlSessionManager;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.core.AbstractJobConfig;
import io.kyligence.kap.job.core.config.FileJobConfig;
import io.kyligence.kap.job.dao.JobInfoDao;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.mapper.JobLockMapper;
import io.kyligence.kap.metadata.transaction.SpringManagedTransactionFactory;
import io.kyligence.kap.rest.delegate.ModelMetadataBaseInvoker;
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
            AbstractJobConfig jobConfig = new FileJobConfig();
            jobConfig.setJobSchedulerMasterPollIntervalSec(1L);
            jobConfig.setJobSchedulerSlavePollIntervalSec(2L);
            jobContext = new JobContext();
            jobContext.setJobConfig(jobConfig);
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
            jobInfoMapper = sqlSessionManager.getMapper(JobInfoMapper.class);
            jobInfoMapper.deleteAllJob();
            jobLockMapper = sqlSessionManager.getMapper(JobLockMapper.class);
            jobLockMapper.deleteAllJobLock();
        } catch (Exception e) {
            throw new RuntimeException("initialize mybatis mappers failed", e);
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
