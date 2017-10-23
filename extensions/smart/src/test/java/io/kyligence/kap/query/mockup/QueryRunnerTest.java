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

package io.kyligence.kap.query.mockup;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;
import io.kyligence.kap.smart.query.SQLResult;

public class QueryRunnerTest extends LocalFileMetadataTestCase {
    @After
    public void cleanup() throws IOException {
        cleanAfterClass();
    }

    @Before
    public void setup() throws IOException {
        createTestMetadata();
    }

    @Test
    public void test() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc("ci_inner_join_cube");
        String[] sqls = { "select count(*) from test_kylin_fact", "select 1", "abcd",
                "select a,b,c from test_kylin_fact", "select price from test_kylin_fact group by price" };

        try (AbstractQueryRunner runner = QueryRunnerFactory.createForCubeSuggestion(config, sqls, 1,
                cubeDesc.getProject())) {
            runner.execute();

            List<SQLResult> results = runner.getQueryResults();
            Assert.assertEquals(SQLResult.Status.SUCCESS, results.get(0).getStatus());
            Assert.assertEquals(SQLResult.Status.SUCCESS, results.get(1).getStatus());
            Assert.assertEquals(SQLResult.Status.FAILED, results.get(2).getStatus());
            Assert.assertEquals(SQLResult.Status.FAILED, results.get(3).getStatus());
            Assert.assertEquals(SQLResult.Status.FAILED, results.get(4).getStatus());
        }
    }
}
