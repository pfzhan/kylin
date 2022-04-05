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

import static io.kyligence.kap.query.engine.QueryRoutingEngine.SPARK_MEM_LIMIT_EXCEEDED;


import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

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
            Assert.assertTrue(
                    ResourceStore.getKylinMetaStore(kylinconfig) instanceof InMemResourceStore);
            return PushdownResult.emptyResult();
        }).when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        queryRoutingEngine.queryWithSqlMassage(queryParams);
        Assert.assertTrue(QueryContext.current().getQueryTagInfo().isPushdown());

        Assert.assertEquals(1, pushdownCount);
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
}
