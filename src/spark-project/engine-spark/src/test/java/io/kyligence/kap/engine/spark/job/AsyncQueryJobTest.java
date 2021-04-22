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
package io.kyligence.kap.engine.spark.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.query.util.QueryParams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsyncQueryJobTest extends NLocalFileMetadataTestCase {

    final static String BUILD_HADOOP_CONF = "kylin.engine.submit-hadoop-conf-dir";
    final static String BUILD_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf";

    final static String ASYNC_HADOOP_CONF = "kylin.query.async-query.submit-hadoop-conf-dir";
    final static String ASYNC_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf_async";
    final static String ASYNC_QUERY_CLASS = "-className io.kyligence.kap.engine.spark.job.AsyncQueryApplication";
    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testAsyncQueryJob_SetHadoopConf() throws ExecuteException, JsonProcessingException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        overwriteSystemProp(BUILD_HADOOP_CONF, BUILD_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                protected ExecuteResult runSparkSubmit(String hadoopConf,
                                                       String jars,
                                                       String kylinJobJar,
                                                       String appArgs) {
                    Assert.assertEquals(BUILD_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    String cmd = generateSparkCmd(hadoopConf, jars, kylinJobJar, appArgs);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());
        }

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                protected ExecuteResult runSparkSubmit(String hadoopConf,
                                                       String jars,
                                                       String kylinJobJar,
                                                       String appArgs) {
                    Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    String cmd = generateSparkCmd(hadoopConf, jars, kylinJobJar, appArgs);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());
        }
    }
}
