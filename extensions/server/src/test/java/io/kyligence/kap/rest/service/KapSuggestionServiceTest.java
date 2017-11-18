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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.cube.CubeOptimizeLog;
import io.kyligence.kap.smart.cube.CubeOptimizeLogManager;

public class KapSuggestionServiceTest extends ServiceTestBase {
    @Autowired
    @Qualifier("kapSuggestionService")
    KapSuggestionService kapSuggestionService;

    @Test
    public void testSaveSampleSqlsWithSqls() throws Exception {
        List<String> sqls = Lists.newArrayList("select 1", "select count(*) from test_kylin_fact");
        internalTestSaveSampleSqls(sqls, "ci_inner_join_model");
    }

    @Test
    public void testSaveSampleSqlsWithoutSqls() throws Exception {
        internalTestSaveSampleSqls(Lists.<String> newArrayList(), "ci_inner_join_model");
    }

    private void internalTestSaveSampleSqls(List<String> sqls, String modelName) throws Exception {
        String cubeName = String.format("%s_%d", modelName, System.currentTimeMillis());
        kapSuggestionService.saveSampleSqls(modelName, cubeName, sqls);

        CubeOptimizeLogManager optimizeLogManager = CubeOptimizeLogManager.getInstance(getTestConfig());
        CubeOptimizeLog cubeOptimizeLog = optimizeLogManager.getCubeOptimizeLog(cubeName);
        Assert.assertNotNull(cubeOptimizeLog);
        Assert.assertEquals(sqls.size(), cubeOptimizeLog.getSqlResult().size());
    }
}
