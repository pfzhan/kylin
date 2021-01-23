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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.v2.OptRecV2TestBase;
import io.kyligence.kap.rest.request.OptRecRequest;

public class OptRecServiceGeneralTest extends OptRecV2TestBase {

    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    public OptRecServiceGeneralTest() {
        super("../server-base/src/test/resources/ut_rec_v2/general",
                new String[] { "cca38043-5e04-4954-b917-039ba37f159e" });
    }

    @Test
    public void testApprove() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableMap.of(100000, "COUNT_ALL", 100001, "MEASURE_AUTO_1"),
                extractIdToName(dataModel.getEffectiveMeasures()));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testDiscard() throws Exception {
        List<Integer> addLayoutid = ImmutableList.of(3);
        List<Integer> removeLayout = Lists.newArrayList(8);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutid, removeLayout);
        prepare(addLayoutid);
        optRecService.discard(getProject(), recRequest);

        Assert.assertEquals(RawRecItem.RawRecState.DISCARD, jdbcRawRecStore.queryById(3).getState());
        Assert.assertEquals(RawRecItem.RawRecState.DISCARD, jdbcRawRecStore.queryById(8).getState());
    }

    @Test
    public void testApproveRemoveLayout() throws IOException {
        //addLayout
        List<Integer> addLayoutId = Lists.newArrayList(3);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        //remove
        List<Integer> removeLayoutId = Lists.newArrayList(8);
        OptRecRequest removeQuest = buildOptRecRequest(ImmutableList.of(), removeLayoutId);
        optRecService.approve(getProject(), removeQuest);

        checkIndexPlan(ImmutableList.of(), getIndexPlan());

        Assert.assertEquals(RawRecItem.RawRecState.APPLIED, jdbcRawRecStore.queryById(8).getState());
    }

    @Test
    public void testApproveWithRename() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, ImmutableMap.of(-2, "KE_TEST"));
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());
        Assert.assertEquals("KE_TEST", dataModel.getMeasureNameByMeasureId(100001));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveBatch() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).add(ImmutableList.of(11, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveBatchWithRename() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, ImmutableMap.of(-2, "KE_TEST_1", -5, "KE_TEST_2"));
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());
        Assert.assertEquals("KE_TEST_1", dataModel.getMeasureNameByMeasureId(100001));
        Assert.assertEquals("KE_TEST_2", dataModel.getMeasureNameByMeasureId(100002));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).add(ImmutableList.of(11, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveWithTwiceRequest() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(Lists.newArrayList(3));
        optRecService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        addLayoutId = Lists.newArrayList(6);
        OptRecRequest recRequest2 = buildOptRecRequest(addLayoutId);
        optRecService.approve(getProject(), recRequest2);

        dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        layoutColOrder = ImmutableList.<List<Integer>> builder().add(ImmutableList.of(0, 100000, 100001))
                .add(ImmutableList.of(11, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

    }

    @Test
    public void testApproveWithInverseTwiceRequest() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(Lists.newArrayList(6));
        optRecService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(11, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        addLayoutId = Lists.newArrayList(3);
        OptRecRequest recRequest2 = buildOptRecRequest(addLayoutId);
        optRecService.approve(getProject(), recRequest2);

        dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        layoutColOrder = ImmutableList.<List<Integer>> builder().add(ImmutableList.of(11, 100000, 100001))
                .add(ImmutableList.of(0, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

    }

    @Test
    public void testApproveReuseDimAndMeasure() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6, 7);

        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(addLayoutId);
        optRecService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).add(ImmutableList.of(11, 100000, 100002))
                .add(ImmutableList.of(0, 100000, 100002)).build();
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
