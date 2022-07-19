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
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.v2.OptRecV2TestBase;
import io.kyligence.kap.rest.request.OptRecRequest;
import io.kyligence.kap.rest.response.OptRecResponse;

public class OptRecServiceCcConflictTest extends OptRecV2TestBase {

    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    public OptRecServiceCcConflictTest() {
        super("src/test/resources/ut_rec_v2/cc_expr_conflict",
                new String[] { "a58135aa-20a6-a296-a55a-9d6e76b02f10" });
    }

    @Test
    public void testApproveWithDupCCExpr() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 8, 13);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        OptRecResponse optRecResponse = optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertFalse(dataModel.getComputedColumnDescs().isEmpty());
        ComputedColumnDesc computedColumnDesc = dataModel.getComputedColumnDescs().get(0);
        Assert.assertEquals("SSB.P_LINEORDER", computedColumnDesc.getTableIdentity());
        Assert.assertEquals("`P_LINEORDER`.`LO_EXTENDEDPRICE` * `P_LINEORDER`.`LO_DISCOUNT` + 1",
                computedColumnDesc.getInnerExpression());
        Assert.assertEquals(2, dataModel.getEffectiveDimensions().size());
        Assert.assertEquals(2, dataModel.getEffectiveMeasures().size());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }

    private void checkIndexPlan(List<List<Integer>> layoutColOrder, IndexPlan actualPlan) {
        Assert.assertEquals(layoutColOrder.size(), actualPlan.getAllLayouts().size());
        Assert.assertEquals(layoutColOrder,
                actualPlan.getAllLayouts().stream().map(LayoutEntity::getColOrder).collect(Collectors.toList()));
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId) {
        return buildOptRecRequest(addLayoutId, ImmutableList.of(), ImmutableMap.of());
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, Map<Integer, String> nameMap) {
        return buildOptRecRequest(addLayoutId, ImmutableList.of(), nameMap);
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, List<Integer> removeLayoutId) {
        return buildOptRecRequest(addLayoutId, removeLayoutId, ImmutableMap.of());
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, List<Integer> removeLayoutId,
            Map<Integer, String> nameMap) {
        OptRecRequest recRequest = new OptRecRequest();
        recRequest.setModelId(getDefaultUUID());
        recRequest.setProject(getProject());
        recRequest.setRecItemsToAddLayout(addLayoutId);
        recRequest.setRecItemsToRemoveLayout(removeLayoutId);
        recRequest.setNames(nameMap);
        return recRequest;
    }
}
