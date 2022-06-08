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
package io.kyligence.kap.rest.service;

import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_DIST_META_URL;
import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_JOB_ID;
import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_QUERY_CONTEXT;
import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_QUERY_PARAMS;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.rest.service.AsyncQueryJob;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.job.dao.JobInfoDao;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.rest.JobMapperFilter;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
@PowerMockIgnore({ "javax.management.*", "javax.script.*" })
public class AsyncQueryJobTest extends NLocalFileMetadataTestCase {

    final static String BUILD_HADOOP_CONF = "kylin.engine.submit-hadoop-conf-dir";
    final static String BUILD_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf";

    final static String ASYNC_HADOOP_CONF = "kylin.query.async-query.submit-hadoop-conf-dir";
    final static String ASYNC_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf_async";
    final static String ASYNC_QUERY_CLASS = "-className io.kyligence.kap.query.engine.AsyncQueryApplication";
    final static String ASYNC_QUERY_SPARK_EXECUTOR_CORES = "kylin.query.async-query.spark-conf.spark.executor.cores";
    final static String ASYNC_QUERY_SPARK_EXECUTOR_MEMORY = "kylin.query.async-query.spark-conf.spark.executor.memory";
    final static String ASYNC_QUERY_SPARK_QUEUE = "root.quard";

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        // Use thenAnswer instead of thenReturn, a workaround for https://github.com/powermock/powermock/issues/992
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenAnswer((invocation -> userGroupInformation));
        PowerMockito.when(UserGroupInformation.getLoginUser()).thenAnswer((invocation -> userGroupInformation));
        PowerMockito.mockStatic(SpringContext.class);
        JobInfoMapper jobInfoMapper = Mockito.spy(JobInfoMapper.class);
        Mockito.when(jobInfoMapper.selectByJobFilter(Mockito.any(JobMapperFilter.class))).thenReturn(new ArrayList<>());
        JobInfoDao jobInfoDao = Mockito.spy(JobInfoDao.class);
        ReflectionTestUtils.setField(jobInfoDao, "jobInfoMapper", jobInfoMapper);
        PowerMockito.when(SpringContext.getBean(JobInfoDao.class)).thenAnswer(invocation -> jobInfoDao);

        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testAsyncQueryJob() throws ExecuteException, JsonProcessingException, ShellException {
        CliCommandExecutor executor = Mockito.spy(new CliCommandExecutor());
        Mockito.doReturn(new CliCommandExecutor.CliCmdExecResult(0, "mock", "mock")).when(executor)
                .execute(Mockito.any(), Mockito.any(), Mockito.any());
        AsyncQueryJob asyncQueryJob = Mockito.spy(new AsyncQueryJob());
        Assert.assertNotNull(asyncQueryJob.getCliCommandExecutor());
        Mockito.doNothing().when(asyncQueryJob).killOrphanApplicationIfExists(Mockito.any());
        Mockito.doReturn(executor).when(asyncQueryJob).getCliCommandExecutor();

        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        asyncQueryJob.setProject(queryParams.getProject());
        Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());

        Mockito.doReturn(null).when(asyncQueryJob).getCliCommandExecutor();
        Assert.assertFalse(asyncQueryJob.submit(queryParams).succeed());
    }

    @Test
    public void testAsyncQueryJob_SetHadoopConf() throws ExecuteException, JsonProcessingException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        overwriteSystemProp(BUILD_HADOOP_CONF, BUILD_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(BUILD_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));

                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());
        }

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());
        }
    }

    @Test
    public void testDumpMetadataAndCreateArgsFile() throws ExecuteException, IOException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        queryParams.setSparkQueue(ASYNC_QUERY_SPARK_QUEUE);
        queryParams.setAclInfo(new QueryContext.AclInfo("user1", Sets.newHashSet("group1"), false));
        QueryContext queryContext = QueryContext.current();
        queryContext.setUserSQL("select 1");
        queryContext.getMetrics().setServer("localhost");
        queryContext.getQueryTagInfo().setAsyncQuery(true);

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
            @Override
            protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                val desc = this.getSparkAppDesc();
                desc.setHadoopConfDir(hadoopConf);
                desc.setKylinJobJar(kylinJobJar);
                desc.setAppArgs(appArgs);
                String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                return ExecuteResult.createSucceed(appArgs
                        .substring(appArgs.lastIndexOf("file:") + "file:".length(), appArgs.lastIndexOf("/")).trim());
            }
        };
        asyncQueryJob.setProject(queryParams.getProject());
        ExecuteResult executeResult = asyncQueryJob.submit(queryParams);
        Assert.assertTrue(executeResult.succeed());
        String asyncQueryJobPath = executeResult.output();
        FileSystem workingFileSystem = HadoopUtil.getWorkingFileSystem();
        Assert.assertTrue(workingFileSystem.exists(new Path(asyncQueryJobPath)));
        Assert.assertEquals(2, workingFileSystem.listStatus(new Path(asyncQueryJobPath)).length);

        FileStatus[] jobFileStatuses = workingFileSystem.listStatus(new Path(asyncQueryJobPath));
        Comparator<FileStatus> fileStatusComparator = new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus o1, FileStatus o2) {
                return o1.getPath().toString().compareTo(o2.getPath().toString());
            }
        };
        Arrays.sort(jobFileStatuses, fileStatusComparator);

        // validate spark job args
        testSparkArgs(asyncQueryJobPath, workingFileSystem.open(jobFileStatuses[1].getPath()), jobFileStatuses[1]);

        FileStatus[] metaFileStatus = workingFileSystem.listStatus(jobFileStatuses[0].getPath());
        Arrays.sort(metaFileStatus, fileStatusComparator);

        // validate kylin properties
        testKylinConfig(workingFileSystem, metaFileStatus[0]);

        // validate metadata
        testMetadata(workingFileSystem, metaFileStatus[1]);
    }

    private void testMetadata(FileSystem workingFileSystem, FileStatus metaFileStatus) throws IOException {
        val rawResourceMap = Maps.<String, RawResource> newHashMap();
        FileStatus metadataFile = metaFileStatus;
        try (FSDataInputStream inputStream = workingFileSystem.open(metadataFile.getPath());
                ZipInputStream zipIn = new ZipInputStream(inputStream)) {
            ZipEntry zipEntry = null;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                if (!zipEntry.getName().startsWith("/")) {
                    continue;
                }
                long t = zipEntry.getTime();
                RawResource raw = new RawResource(zipEntry.getName(), ByteSource.wrap(IOUtils.toByteArray(zipIn)), t,
                        0);
                rawResourceMap.put(zipEntry.getName(), raw);
            }
        }
        Assert.assertEquals(81, rawResourceMap.size());
    }

    private void testKylinConfig(FileSystem workingFileSystem, FileStatus metaFileStatus) throws IOException {
        FileStatus kylinPropertiesFile = metaFileStatus;
        Properties properties = new Properties();
        try (FSDataInputStream inputStream = workingFileSystem.open(kylinPropertiesFile.getPath())) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
            properties.load(br);
        }
        Assert.assertTrue(properties.size() > 0);
        Assert.assertFalse(properties.getProperty("kylin.query.queryhistory.url").contains("hdfs"));
        Assert.assertEquals(ASYNC_QUERY_SPARK_QUEUE,
                properties.getProperty("kylin.query.async-query.spark-conf.spark.yarn.queue"));
        Assert.assertEquals("5", properties.getProperty(ASYNC_QUERY_SPARK_EXECUTOR_CORES));
        Assert.assertEquals("12288m", properties.getProperty(ASYNC_QUERY_SPARK_EXECUTOR_MEMORY));
    }

    private void testSparkArgs(String asyncQueryJobPath, FSDataInputStream open, FileStatus jobFileStatus)
            throws IOException {
        String argsLine = null;
        try (FSDataInputStream inputStream = open) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
            argsLine = br.readLine();
        }
        Assert.assertNotNull(argsLine);
        Map<String, String> argsMap = JsonUtil.readValueAsMap(argsLine);
        Assert.assertTrue(argsMap.get(P_JOB_ID).startsWith(ASYNC_QUERY_JOB_ID_PRE));
        QueryParams readQueryParams = JsonUtil.readValue(argsMap.get(P_QUERY_PARAMS), QueryParams.class);
        Assert.assertEquals("select 1", readQueryParams.getSql());
        Assert.assertEquals(ASYNC_QUERY_SPARK_QUEUE, readQueryParams.getSparkQueue());
        Assert.assertEquals("default", readQueryParams.getProject());
        Assert.assertEquals("user1", readQueryParams.getAclInfo().getUsername());
        Assert.assertEquals("[group1]", readQueryParams.getAclInfo().getGroups().toString());
        QueryContext readQueryContext = JsonUtil.readValue(argsMap.get(P_QUERY_CONTEXT), QueryContext.class);
        Assert.assertEquals("select 1", readQueryContext.getUserSQL());
        Assert.assertEquals("localhost", readQueryContext.getMetrics().getServer());
        Assert.assertTrue(readQueryContext.getQueryTagInfo().isAsyncQuery());
        Assert.assertTrue(argsMap.get(P_DIST_META_URL).contains(
                asyncQueryJobPath.substring(asyncQueryJobPath.indexOf("working-dir/") + "working-dir/".length())));
    }

    @Test
    public void testJobNotModifyKylinConfig() throws ExecuteException, IOException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(
                            appArgs.substring(appArgs.lastIndexOf("file:") + "file:".length(), appArgs.lastIndexOf("/"))
                                    .trim());
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            ExecuteResult executeResult = asyncQueryJob.submit(queryParams);
            Assert.assertTrue(executeResult.succeed());
        }
        Assert.assertFalse(KylinConfig.getInstanceFromEnv().getMetadataUrl().toString().contains("hdfs"));
    }

    @Test
    public void testJobSparkCmd() throws ExecuteException, IOException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        queryParams.setSparkQueue(ASYNC_QUERY_SPARK_QUEUE);

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        overwriteSystemProp(ASYNC_QUERY_SPARK_EXECUTOR_CORES, "3");
        overwriteSystemProp(ASYNC_QUERY_SPARK_EXECUTOR_MEMORY, "513m");
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            ExecuteResult executeResult = asyncQueryJob.submit(queryParams);
            Assert.assertTrue(executeResult.succeed());
            Assert.assertTrue(executeResult.output().contains("--conf 'spark.executor.memory=513m'"));
            Assert.assertTrue(executeResult.output().contains("--conf 'spark.executor.cores=3'"));
        }
    }
}
