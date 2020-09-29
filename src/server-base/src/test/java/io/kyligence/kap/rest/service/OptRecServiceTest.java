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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.v2.OptRecV2TestBase;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;

public class OptRecServiceTest extends OptRecV2TestBase {

    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    public OptRecServiceTest() {
        super("../server-base/src/test/resources/ut_rec_v2/opt_service",
                new String[] { "db89adb4-3aad-4f2a-ac2e-72ea0a30420b" });
    }

    @Test
    public void testGetOptRecRequest() throws IOException {
        // test get all
        prepareAllLayoutRecs();
        OptRecLayoutsResponse recResp1 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(28, recResp1.getLayouts().size());

        // set topN to 50, get all and assert
        changeRecTopN(50);
        OptRecLayoutsResponse recResp2 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(35, recResp2.getLayouts().size());

        // only get add_table_index
        OptRecLayoutsResponse recResp3 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 0, 10);
        Assert.assertEquals(1, recResp3.getLayouts().size());

        // test limit
        OptRecLayoutsResponse recResp4 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX", "ADD_TABLE_INDEX"), null, false, "", 0, 30);
        Assert.assertEquals(27, recResp4.getLayouts().size());
        recResp4 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, false, "", 0, 20);
        Assert.assertEquals(20, recResp4.getLayouts().size());

        // test offset
        OptRecLayoutsResponse recResp5 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 1, 10);
        Assert.assertTrue(recResp5.getLayouts().isEmpty());
        recResp5 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 0, 10);
        Assert.assertEquals(1, recResp5.getLayouts().size());

        // test orderBy
        OptRecLayoutsResponse recResp6 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, false, "usage", 0, 30);
        Assert.assertEquals(89, recResp6.getLayouts().get(recResp6.getLayouts().size() - 1).getId());
        recResp6 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, true, "usage", 0, 30);
        Assert.assertEquals(89, recResp6.getLayouts().get(0).getId());
    }

    private void prepareAllLayoutRecs() throws IOException {
        prepare(Lists.newArrayList(2, 3, 6, 10, 24, 59, 60, 61, 62, 76, 77, 78, 79, 80, 82, 83, 84, 85, 87, 88, 89, 91,
                92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104));
    }

    @Test
    public void testApproveAll() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        UnitOfWork.doInTransactionWithRetry(() -> {
            optRecService.batchApprove(getProject(), "all");
            return 0;
        }, "");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(56, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(21, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveAllRemovalRecItems() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        UnitOfWork.doInTransactionWithRetry(() -> {
            optRecService.batchApprove(getProject(), "REMOVE_INDEX");
            return 0;
        }, "");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(7, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(1, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveAllAdditionalRecItems() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        changeRecTopN(50);
        UnitOfWork.doInTransactionWithRetry(() -> {
            optRecService.batchApprove(getProject(), "ADD_INDEX");
            return 0;
        }, "");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(36, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveOneModel() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());

        changeRecTopN(50);
        UnitOfWork.doInTransactionWithRetry(() -> {
            optRecService.batchApprove(getProject(), Lists.newArrayList(modelBeforeApprove.getAlias()), "all");
            return 0;
        }, "");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }
}
