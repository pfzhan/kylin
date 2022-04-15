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

package io.kyligence.kap.query.engine;

import static io.kyligence.kap.query.engine.QueryRoutingEngine.SPARK_JOB_FAILED;
import static io.kyligence.kap.query.engine.QueryRoutingEngine.SPARK_MEM_LIMIT_EXCEEDED;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.query.QueryExtension;

public class QueryRoutingEngineTest extends NLocalFileMetadataTestCase {

    private int pushdownCount;
    @Mock
    private QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        pushdownCount = 0;
        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    @Test
    public void testQueryPushDown() throws Throwable {
        Assert.assertEquals(0, pushdownCount);
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doAnswer(invocation -> {
            pushdownCount++;
            Assert.assertTrue(ResourceStore.getKylinMetaStore(kylinconfig) instanceof InMemResourceStore);
            return PushdownResult.emptyResult();
        }).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        queryRoutingEngine.queryWithSqlMassage(queryParams);
        Assert.assertTrue(QueryContext.current().getQueryTagInfo().isPushdown());
        Assert.assertEquals(1, pushdownCount);

        //to cover force push down
        queryParams.setForcedToPushDown(true);

        Mockito.doAnswer(invocation -> {
            pushdownCount++;
            Assert.assertTrue(ResourceStore.getKylinMetaStore(kylinconfig) instanceof InMemResourceStore);
            return PushdownResult.emptyResult();
        }).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        queryRoutingEngine.queryWithSqlMassage(queryParams);
        Assert.assertTrue(QueryContext.current().getQueryTagInfo().isPushdown());
        Assert.assertEquals(2, pushdownCount);

        // Throw Exception When push down
        Mockito.doThrow(new KylinException(QueryErrorCode.SCD2_DUPLICATE_JOIN_COL, "")).when(queryRoutingEngine)
                .tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }

        Mockito.doThrow(new Exception("")).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(),
                Mockito.anyBoolean());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }

    }

    @Test
    public void testThrowExceptionWhenSparkOOM() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doThrow(new SparkException(
                "Job aborted due to stage failure: Task 40 in stage 888.0 failed 1 times, most recent failure: "
                        + "Lost task 40.0 in stage 888.0 (TID 79569, hrbd-73, executor 5): ExecutorLostFailure (executor 5 exited "
                        + "caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits.  6.5 GB "
                        + "of 6.5 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead or disabling "
                        + "yarn.nodemanager.vmem-check-enabled because of YARN-4714."))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SparkException || e.getCause() instanceof SparkException);
            Assert.assertTrue(e.getMessage().contains(SPARK_MEM_LIMIT_EXCEEDED)
                    || e.getCause().getMessage().contains(SPARK_MEM_LIMIT_EXCEEDED));
        }
    }

    @Test
    public void testThrowExceptionWhenSparkJobFailed() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);
        //to cover PrepareStatement
        queryParams.setPrepare(true);
        queryParams.setPrepareStatementWithParams(true);
        PrepareSqlStateParam[] params = new PrepareSqlStateParam[11];
        params[0] = new PrepareSqlStateParam(String.class.getName(), "1");
        params[1] = new PrepareSqlStateParam(Integer.class.getName(), "1");
        params[2] = new PrepareSqlStateParam(Short.class.getName(), "1");
        params[3] = new PrepareSqlStateParam(Long.class.getName(), "1");
        params[4] = new PrepareSqlStateParam(Float.class.getName(), "1.1");
        params[5] = new PrepareSqlStateParam(Double.class.getName(), "1.1");
        params[6] = new PrepareSqlStateParam(Boolean.class.getName(), "1");
        params[7] = new PrepareSqlStateParam(Byte.class.getName(), "1");
        params[8] = new PrepareSqlStateParam(Date.class.getName(), "2022-02-22");
        params[9] = new PrepareSqlStateParam(Time.class.getName(), "22:22:22");
        params[10] = new PrepareSqlStateParam(Timestamp.class.getName(), "2022-02-22 22:22:22.22");
        queryParams.setParams(params);

        Mockito.doThrow(
                new TransactionException("", new Throwable(new SparkException("Job aborted due to stage failure: "))))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(
                    e.getCause() instanceof SparkException || e.getCause().getCause() instanceof SparkException);
            Assert.assertTrue(e.getCause().getMessage().contains(SPARK_JOB_FAILED)
                    || e.getCause().getCause().getMessage().contains(SPARK_JOB_FAILED));
            Assert.assertFalse(QueryContext.current().getQueryTagInfo().isPushdown());
        }
    }

    @Test
    public void testThrowExceptionWhenNoStreamingRealizationFound() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        Mockito.doThrow(new TransactionException("", new Throwable(new NoStreamingRealizationFoundException(""))))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof NoStreamingRealizationFoundException
                    || e.getCause().getCause() instanceof NoStreamingRealizationFoundException);
            Assert.assertFalse(QueryContext.current().getQueryTagInfo().isPushdown());
        }
    }

}
