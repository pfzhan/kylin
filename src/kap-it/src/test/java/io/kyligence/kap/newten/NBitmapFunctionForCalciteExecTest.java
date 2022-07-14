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

package io.kyligence.kap.newten;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.query.util.QueryParams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.query.engine.QueryRoutingEngine;
import io.kyligence.kap.query.engine.data.QueryResult;

public class NBitmapFunctionForCalciteExecTest extends NLocalWithSparkSessionTest {

    private Logger logger = LoggerFactory.getLogger(NBitmapFunctionForCalciteExecTest.class);

    @Mock
    private QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);

    @Before
    public void setup() {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        populateSSWithCSVData(getTestConfig(), getProject(), ss);
        Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", "true");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally");
    }

    @Override
    public String getProject() {
        return "intersect_count";
    }

    @Test
    public void testIntersectCountForFalseFilter() throws Exception {
        logger.info("comming....");
        String query = "select "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-.*GTC', 'Others'], 'REGEXP') as b, "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as c "
                + "from test_kylin_fact where 1=2";
        QueryParams queryParams = new QueryParams();
        queryParams.setProject(getProject());
        queryParams.setSql(query);
        queryParams.setKylinConfig(getTestConfig());
        queryParams.setSelect(true);
        logger.info("comming....2222 queryRoutingEngine:" + queryRoutingEngine);
        QueryResult result = queryRoutingEngine.queryWithSqlMassage(queryParams);
        List<String> rows = result.getRows().get(0);
        Assert.assertEquals("null", rows.get(0));
        Assert.assertEquals("null", rows.get(1));
    }

}
