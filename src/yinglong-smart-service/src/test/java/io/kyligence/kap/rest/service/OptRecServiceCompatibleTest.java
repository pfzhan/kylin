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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.v2.OptRecV2TestBase;
import io.kyligence.kap.rest.response.OpenRecApproveResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;

public class OptRecServiceCompatibleTest extends OptRecV2TestBase {

    OptRecService optRecService = Mockito.spy(new OptRecService());
    ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setup() throws Exception {
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        prepareACL();
        QueryHistoryTaskScheduler.getInstance(getProject()).init();
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        QueryHistoryTaskScheduler.shutdownByProject(getProject());
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "modelService", modelService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    public OptRecServiceCompatibleTest() {
        super("../yinglong-smart-service/src/test/resources/ut_rec_v2/compatible_test",
                new String[] { "13aceef1-d9a1-4ec0-bb8e-b52f7bd3b99b" });
    }

    /**
     * 1. Following query statements were used to prepare recommendations.
     * [a]  select lo_orderkey, lo_extendedprice from ssb.p_lineorder group by lo_extendedprice, lo_orderkey
     * [b]  select lo_orderkey, lo_extendedprice * lo_quantity from ssb.p_lineorder
     * [c]  select lo_orderkey, lo_extendedprice * lo_quantity, lo_quantity from ssb.p_lineorder
     * [d]  select lo_orderkey, lo_tax, lo_quantity from ssb.p_lineorder
     * [e]  select lo_orderkey, sum(lo_extendedprice) from ssb.p_lineorder group by lo_orderkey
     * [f]  select lo_orderkey, lo_commitdate, sum(lo_extendedprice * lo_quantity), count(lo_quantity) from ssb.p_lineorder group by lo_orderkey, lo_commitdate
     * [g]  select lo_orderkey, lo_linenumber, sum(lo_extendedprice * lo_quantity), count(lo_shippriotity) from ssb.p_lineorder group by lo_orderkey, lo_linenumber
     * [h]  select lo_orderkey, lo_shipmode, sum(lo_extendedprice * lo_quantity), count(lo_tax) from ssb.p_lineorder group by lo_orderkey, lo_shipmode
     *
     * 2. Then put the column `lo_extendedprice` after `lo_orderkey` and reload table to prepare new recommendations again.
     * 3. Approve these recommendations. This process will validate these recommendations first,
     *    the comment after each query statement is the validation result.
     */
    @Test
    public void testApproveAll() throws IOException {
        reloadTable();
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(0, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(3, modelBeforeApprove.getRecommendationsCount());
        Assert.assertEquals(0, getIndexPlan().getAllLayouts().size());
        Assert.assertEquals(0, getIndexPlan().getAllLayoutsReadOnly().size());
        OptRecLayoutsResponse response = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "all");
        Assert.assertEquals(14, response.getLayouts().size());
        jdbcRawRecStore.queryAll().forEach(rawRecItem -> {
            if (rawRecItem.isLayoutRec()) {
                Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, rawRecItem.getState());
            }
        });

        List<OpenRecApproveResponse.RecToIndexResponse> recToIndexResponses = Lists.newArrayList();
        UnitOfWork.doInTransactionWithRetry(() -> {
            recToIndexResponses.addAll(optRecService.batchApprove(getProject(), Lists.newArrayList(), "all", false));
            return 0;
        }, getProject());

        Assert.assertEquals(1, recToIndexResponses.size());
        Assert.assertEquals(7, recToIndexResponses.get(0).getAddedIndexes().size());

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(8, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(6, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(0, modelAfterApprove.getRecommendationsCount());
        Assert.assertEquals(7, getIndexPlan().getAllLayoutsMap().size());
        Assert.assertEquals(7, getIndexPlan().getAllLayoutsReadOnly().size());
    }

    private void prepareAllLayoutRecs() throws IOException {
        prepare(Lists.newArrayList(15, 16, 17, 18, 19, 20, 21, 33, 34, 35, 36, 37, 38, 39));
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }

    private void reloadTable() throws IOException {
        String basePath = getBasePath();
        String tableJson = "table/SSB.P_LINEORDER.json";
        TableDesc tableDesc = JsonUtil.readValue(new File(basePath, tableJson), TableDesc.class);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager.getInstance(getTestConfig(), getProject()).updateTableDesc(tableDesc);
            return null;
        }, getProject());
    }
}
