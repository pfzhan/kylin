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


import java.sql.SQLException;
import java.util.List;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.util.QueryParams;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class QueryRoutingEngineTest extends NLocalFileMetadataTestCase {

    @Mock
    private QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testQueryPushdownOnRealizationNotFound() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        final NoRealizationFoundException cause = new NoRealizationFoundException("");
        Mockito.doThrow(new SQLException(cause))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());
        final Pair<List<List<String>>, List<SelectedColumnMeta>> pushdownResult = new Pair<>();
        Mockito.doReturn(pushdownResult)
                .when(queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
        Assert.assertEquals(pushdownResult, queryRoutingEngine.queryWithSqlMassage(queryParams));

        Mockito.doThrow(new TransactionException("", new SQLException(cause)))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());
        Assert.assertEquals(pushdownResult, queryRoutingEngine.queryWithSqlMassage(queryParams));
    }

    @Test
    public void testQueryRethrowOtherException() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();

        QueryParams queryParams = new QueryParams();
        queryParams.setProject(project);
        queryParams.setSql(sql);
        queryParams.setKylinConfig(kylinconfig);
        queryParams.setSelect(true);

        final SparkException cause = new SparkException("");
        Mockito.doThrow(new SQLException(cause))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertEquals(cause, e.getCause());
        }

        Mockito.doThrow(new TransactionException("", new SQLException(cause)))
                .when(queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());
        try {
            queryRoutingEngine.queryWithSqlMassage(queryParams);
        } catch (Exception e) {
            Assert.assertEquals(cause, e.getCause().getCause());
        }
    }
}