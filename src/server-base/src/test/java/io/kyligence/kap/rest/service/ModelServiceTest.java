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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.rest.execution.SucceedTestExecutable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.response.CuboidStatus;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NSpanningTreeResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SimplifiedColumnResponse;
import lombok.val;

public class ModelServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private SegmentHelper segmentHelper = new SegmentHelper();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @Before
    public void setup() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        modelService.setSemanticUpdater(semanticService);
        modelService.setSegmentHelper(segmentHelper);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetModels() {

        List<NDataModelResponse> models2 = modelService.getModels("nmodel_full_measure_test", "default", false, "", "",
                "last_modify", true);
        Assert.assertEquals(1, models2.size());
        List<NDataModelResponse> model3 = modelService.getModels("nmodel_full_measure_test", "default", true, "", "",
                "last_modify", true);
        Assert.assertEquals(1, model3.size());
        List<NDataModelResponse> model4 = modelService.getModels("nmodel_full_measure_test", "default", false, "adm",
                "", "last_modify", true);
        Assert.assertEquals(1, model4.size());
        List<NDataModelResponse> model5 = modelService.getModels("nmodel_full_measure_test", "default", false, "adm",
                "DISABLED", "last_modify", true);
        Assert.assertEquals(0, model5.size());

    }

    @Test
    public void testGetModelsWithCC() {
        List<NDataModelResponse> models = modelService.getModels("nmodel_basic", "default", true, "", "", "", false);
        Assert.assertEquals(1, models.size());
        NDataModelResponse model = models.get(0);
        Assert.assertTrue(model.getSimpleTables().stream().map(t -> t.getColumns()).flatMap(List::stream)
                .anyMatch(SimplifiedColumnResponse::isComputedColumn));
    }

    @Test
    public void testGetSegments() {

        Segments<NDataSegment> segments = modelService.getSegments("nmodel_basic", "default", "0", "" + Long.MAX_VALUE);
        Assert.assertEquals(1, segments.size());
    }

    @Test
    public void testGetAggIndices() {

        List<CuboidDescResponse> indices = modelService.getAggIndices("nmodel_basic", "default");
        Assert.assertEquals(5, indices.size());
        Assert.assertTrue(indices.get(0).getId() < NCuboidDesc.TABLE_INDEX_START_ID);

    }

    @Test
    public void testGetTableIndices() {

        List<CuboidDescResponse> indices = modelService.getTableIndices("nmodel_basic", "default");
        Assert.assertEquals(3, indices.size());
        Assert.assertTrue(indices.get(0).getId() >= NCuboidDesc.TABLE_INDEX_START_ID);

    }

    @Test
    public void testGetCuboidDescs() {

        List<NCuboidDesc> cuboids = modelService.getCuboidDescs("nmodel_basic", "default");
        Assert.assertEquals(8, cuboids.size());
    }

    @Test
    public void testGetCuboidById_AVAILABLE() {
        CuboidDescResponse cuboid = modelService.getCuboidById("nmodel_basic", "default", 0L);
        Assert.assertEquals(0L, cuboid.getId());
        Assert.assertEquals(CuboidStatus.AVAILABLE, cuboid.getStatus());
        Assert.assertEquals(252928L, cuboid.getStorageSize());
    }

    @Test
    public void testGetCuboidById_NoSegments_EMPTYStatus() {
        CuboidDescResponse cuboid = modelService.getCuboidById("ut_inner_join_cube_partial", "default", 13000L);
        Assert.assertEquals(13000L, cuboid.getId());
        Assert.assertEquals(CuboidStatus.EMPTY, cuboid.getStatus());
        Assert.assertEquals(0L, cuboid.getStorageSize());
        Assert.assertEquals(0L, cuboid.getStartTime());
        Assert.assertEquals(0L, cuboid.getEndTime());
    }

    @Test
    public void testGetModelJson() throws IOException {
        String modelJson = modelService.getModelJson("nmodel_basic", "default");
        Assert.assertTrue(JsonUtil.readValue(modelJson, NDataModel.class).getName().equals("nmodel_basic"));
    }

    @Test
    public void testGetModelRelations() {
        List<NSpanningTree> relations = modelService.getModelRelations("nmodel_basic", "default");
        Assert.assertEquals(1, relations.size());

        relations = modelService.getModelRelations("nmodel_basic_inner", "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(6, relations.get(0).getBuildLevel());
        Assert.assertThat(
                relations.get(0).getCuboidsByLayer().stream().map(Collection::size).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList(1, 4, 4, 3, 3, 1, 1)));

        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        cubeMgr.updateCubePlan("ncube_basic_inner", copyForWrite -> {
            val rule = new NRuleBasedCuboidsDesc();
            rule.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            rule.setMeasures(Lists.newArrayList(1001, 1002));
            try {
                val aggGroup = JsonUtil.readValue("{\n" + "        \"includes\": [1, 2, 3],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [],\n"
                        + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [],\n"
                        + "          \"dim_cap\": 1\n" + "        }\n" + "      }", NAggregationGroup.class);
                rule.setAggregationGroups(Lists.newArrayList(aggGroup));
                copyForWrite.setNewRuleBasedCuboid(rule);
            } catch (IOException ignore) {
            }
        });
        relations = modelService.getModelRelations("nmodel_basic_inner", "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(5, relations.get(0).getBuildLevel());
    }

    @Test
    public void testGetSimplifiedModelRelations() {
        List<NSpanningTreeResponse> relations = modelService.getSimplifiedModelRelations("nmodel_basic", "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(1, relations.get(0).getRoots().size());
        Assert.assertEquals(5, relations.get(0).getNodesMap().size());
    }

    @Test
    public void testDropModelException() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("You should purge your model first before you drop it");
        modelService.dropModel("nmodel_basic_inner", "default");
    }

    @Test
    public void testDropModelExceptionName() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.dropModel("nmodel_basic2222", "default");
    }

    @Test
    public void testDropModelPass() {
        modelService.dropModel("test_encoding", "default");
        List<NDataModelResponse> models = modelService.getModels("test_encoding", "default", true, "", "",
                "last_modify", true);
        Assert.assertTrue(CollectionUtils.isEmpty(models));

    }

    @Test
    public void testPurgeModelManually() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("test_encoding");
        NDataModel modelUpdate = modelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        modelService.purgeModelManually("test_encoding", "default");
        List<NDataSegment> segments = modelService.getSegments("test_encoding", "default", "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(CollectionUtils.isEmpty(segments));
    }

    @Test
    public void testPurgeModelManually_TableOriented_Exception() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("test_encoding");
        NDataModel modelUpdate = modelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model 'test_encoding' is table oriented, can not pruge the model!");
        modelService.purgeModelManually("test_encoding", "default");
    }

    @Test
    public void testGetAffectedSegmentsResponse_NoSegments_Exception() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("No segments to refresh, please select new range and try again!");
        List<NDataSegment> segments = modelService.getSegments("test_encoding", "default", "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(CollectionUtils.isEmpty(segments));
        RefreshAffectedSegmentsResponse response = modelService.getAffectedSegmentsResponse("default",
                "DEFAULT.TEST_ENCODING", "0", "12223334", ManagementType.TABLE_ORIENTED);
    }

    @Test
    public void testPurgeModelExceptionName() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.purgeModelManually("nmodel_basic2222", "default");
    }

    @Test
    public void testCloneModelException() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model alias nmodel_basic_inner already exists");
        modelService.cloneModel("nmodel_basic", "nmodel_basic_inner", "default");
    }

    @Test
    public void testCloneModelExceptionName() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.cloneModel("nmodel_basic2222", "nmodel_basic_inner222", "default");
    }

    @Test
    public void testCloneModel() {
        modelService.cloneModel("test_encoding", "test_encoding_new", "default");
        List<NDataModelResponse> models = modelService.getModels("", "default", true, "", "", "last_modify", true);
        Assert.assertEquals(7, models.size());
    }

    @Test
    public void testRenameModel() {
        modelService.renameDataModel("default", "nmodel_basic", "new_name");
        List<NDataModelResponse> models = modelService.getModels("new_name", "default", true, "", "", "last_modify",
                true);
        Assert.assertTrue(models.get(0).getAlias().equals("new_name"));
    }

    @Test
    public void testRenameModelException() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic222' not found");
        modelService.renameDataModel("default", "nmodel_basic222", "new_name");
    }

    @Test
    public void testRenameModelException2() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model alias nmodel_basic_inner already exists");
        modelService.renameDataModel("default", "nmodel_basic", "nmodel_basic_inner");
    }

    @Test
    public void testUpdateDataModelStatus() {
        modelService.updateDataModelStatus("nmodel_full_measure_test", "default", "OFFLINE");
        List<NDataModelResponse> models = modelService.getModels("nmodel_full_measure_test", "default", true, "", "",
                "last_modify", true);
        Assert.assertTrue(models.get(0).getName().equals("nmodel_full_measure_test")
                && models.get(0).getStatus().equals(RealizationStatusEnum.OFFLINE));
    }

    @Test
    public void testUpdateDataModelStatus_ModelNotExist_Exception() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic222' not found");
        modelService.updateDataModelStatus("nmodel_basic222", "default", "OFFLINE");
    }

    @Test
    @Ignore
    public void testUpdateDataModelStatus_NoReadySegments_Exception() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("No ready segment in model 'nmodel_basic_inner', can not online the model!");
        modelService.updateDataModelStatus("nmodel_basic_inner", "default", "ONLINE");
    }

    @Test
    @Ignore("dataflow's checkAllowOnline method is removed")
    public void testUpdateDataModelStatus_SmallerThanQueryRange_Exception() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Some segments in model 'all_fixed_length' are not ready, can not online the model!");
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        dataLoadingRange.setUuid(UUID.randomUUID().toString());
        dataLoadingRange.setColumnName("CAL_DT");
        dataLoadingRange.setActualQueryStart(-1);
        dataLoadingRange.setActualQueryEnd(1309891513769L);
        NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .createDataLoadingRange(dataLoadingRange);
        modelService.updateDataModelStatus("nmodel_basic", "default", "ONLINE");
        modelService.updateDataModelStatus("all_fixed_length", "default", "ONLINE");
    }

    @Test
    public void testGetSegmentRangeByModel() {
        SegmentRange segmentRange = modelService.getSegmentRangeByModel("default", "nmodel_basic", "0", "2322442");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange);
        SegmentRange segmentRange2 = modelService.getSegmentRangeByModel("default", "nmodel_basic", "", "");
        Assert.assertTrue(segmentRange2 instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange2.getStart().equals(0L) && segmentRange2.getEnd().equals(Long.MAX_VALUE));
    }

    @Test
    public void testGetRelatedModels_HasNoErrorJobs() {
        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(modelService.getExecutableManager("default")).thenReturn(executableManager);
        Mockito.when(executableManager.getExecutablesByStatus(ExecutableState.ERROR)).thenReturn(Lists.newArrayList());
        List<RelatedModelResponse> responses = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic");
        Assert.assertEquals(2, responses.size());
        Assert.assertEquals(false, responses.get(0).isHasErrorJobs());
    }

    @Test
    public void testGetRelatedModels_HasErrorJobs() {
        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(modelService.getExecutableManager("default")).thenReturn(executableManager);
        Mockito.when(executableManager.getExecutablesByStatus(ExecutableState.ERROR)).thenReturn(mockJobs());
        List<RelatedModelResponse> responses = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic_inner");
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals(true, responses.get(0).isHasErrorJobs());
    }

    @Test
    public void testGetRelatedModels() {
        List<RelatedModelResponse> models = modelService.getRelateModels("default", "EDW.TEST_CAL_DT", "");
        Assert.assertTrue(models.size() == 0);
        List<RelatedModelResponse> models2 = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic_inner");
        Assert.assertEquals(1, models2.size());
    }

    @Test
    public void testGetRelatedModels_OneModelBasedModel() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val modelUpdate = modelManager.copyForWrite(modelManager.getDataModelDesc("nmodel_basic"));
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        List<RelatedModelResponse> models = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT", "");
        Assert.assertEquals(3, models.size());
        val modelUpdate2 = modelManager.copyForWrite(modelManager.getDataModelDesc("nmodel_basic"));
        modelUpdate2.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate2);
    }

    @Test
    public void testIsModelsUsingTable() {
        boolean result = modelService.isModelsUsingTable("DEFAULT.TEST_KYLIN_FACT", "default");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetModelUsingTable() {

        List<String> result = modelService.getModelsUsingTable("DEFAULT.TEST_KYLIN_FACT", "default");
        Assert.assertEquals(4, result.size());
    }

    @Test
    @Ignore("useless")
    public void testRefreshSegmentsByDataRange() {
        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "9223372036854775807");
        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        List<Event> events = eventDao.getEvents();
        boolean flag = false;
        // TODO check other events
        //        for (Event event : events) {
        //            if (event instanceof LoadingRangeRefreshEvent) {
        //                if (event.getSegmentRange().getStart().toString().equals("0")
        //                        && event.getSegmentRange().getEnd().toString().equals("12223334")) {
        //                    flag = true;
        //                }
        //            }
        //        }
        //        Assert.assertTrue(flag);
    }

    @Test
    public void testRefreshSegments_AffectedSegmentRangeChanged_Exception() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not refersh, please try again and confirm affected storage!");
        RefreshAffectedSegmentsResponse response = new RefreshAffectedSegmentsResponse();
        response.setAffectedStart("12");
        response.setAffectedEnd("120");
        Mockito.doReturn(response).when(modelService).getAffectedSegmentsResponse("default", "DEFAULT.TEST_KYLIN_FACT",
                "0", "12223334", ManagementType.TABLE_ORIENTED);
        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "12223334");
    }

    @Test
    public void testDeleteSegmentById_SegmentToDeleteOverlapsBuilding_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel dataModel = dataModelManager.getDataModelDesc("nmodel_basic_inner");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflow df = dataflowManager.getDataflow("ncube_basic_inner");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("ncube_basic_inner");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not remove segment (ID:" + dataSegment.getId()
                + "), because this segment overlaps building segments!");
        modelService.deleteSegmentById("nmodel_basic_inner", "default", new String[] { dataSegment.getId() });
    }

    @Test
    public void testDeleteSegmentById_TableOrientedModel_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dataflowManager.getDataflow("ncube_basic_inner");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("ncube_basic_inner");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model 'nmodel_basic_inner' is table oriented, can not remove segments manually!");
        modelService.deleteSegmentById("nmodel_basic_inner", "default", new String[] { dataSegment.getId() });
    }

    @Test
    public void testDeleteSegmentById_SegmentToRefreshOverlapsBuilding_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dataflowManager.getDataflow("ncube_basic_inner");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("ncube_basic_inner");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);

        start = SegmentRange.dateToLong("2010-01-02");
        end = SegmentRange.dateToLong("2010-01-03");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflow("ncube_basic_inner");
        val dataSegment2 = dataflowManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        eventDao.deleteAllEvents();
        //refresh normally
        modelService.refreshSegmentById("nmodel_basic_inner", "default", new String[] { dataSegment2.getId() });
        List<Event> events = eventDao.getEvents();
        // TODO check other events
        //        Assert.assertEquals(1, events.size());
        //        Assert.assertEquals(SegmentRange.dateToLong("2010-01-02"), events.get(0).getSegmentRange().getStart());
        //        Assert.assertEquals(SegmentRange.dateToLong("2010-01-03"), events.get(0).getSegmentRange().getEnd());

        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not remove segment (ID:" + dataSegment.getId()
                + "), because this segment overlaps building segments!");
        //refresh exception
        modelService.refreshSegmentById("nmodel_basic_inner", "default", new String[] { dataSegment.getId() });
    }

    @Test
    public void testDeleteSegmentById_UnconsecutiveSegmentsToDelete_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel dataModel = dataModelManager.getDataModelDesc("nmodel_basic_inner");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflow df = dataflowManager.getDataflow("ncube_basic_inner");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        Segments<NDataSegment> segments = new Segments<>();

        long start;
        long end;
        for (int i = 0; i <= 6; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic_inner");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        eventDao.deleteAllEvents();
        //remove normally
        modelService.deleteSegmentById("nmodel_basic_inner", "default", new String[] { segments.get(0).getId() });
        List<Event> events = eventDao.getEvents();
        //2 dataflows
        val df2 = dataflowManager.getDataflowByModelName(dataModel.getName());
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Only consecutive segments in head or tail can be removed!");
        modelService.deleteSegmentById("nmodel_basic_inner", "default",
                new String[] { segments.get(2).getId(), segments.get(3).getId() });
    }

    @Test
    public void testCreateModel_ExistedAlias_Exception() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model alias nmodel_basic already exists!");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setName("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setProject("default");
        modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    @Test
    public void testCreateModel() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setName("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        modelService.createModel(modelRequest.getProject(), modelRequest);
        NDataModel newModel = modelManager.getDataModelDesc("new_model");
        Assert.assertEquals("new_model", newModel.getName());
        modelManager.dropModel(newModel);
    }

    @Test
    public void testCreateModelWithDefaultMeasures() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic");

        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setName("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        modelService.createModel(modelRequest.getProject(), modelRequest);
        NDataModel newModel = modelManager.getDataModelDesc("new_model");
        Assert.assertEquals("new_model", newModel.getName());
        List<NDataModelResponse> models = modelService.getModels("new_model", "default", false, "ADMIN", "", "", false);
        Assert.assertEquals("COUNT_ALL", models.get(0).getSimplifiedMeasures().get(0).getName());
        modelManager.dropModel(newModel);
    }

    @Test
    public void testBuildSegmentsManually_TableOrientedModel_Exception() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Table oriented model 'nmodel_basic' can not build segments manually!");
        modelService.buildSegmentsManually("default", "nmodel_basic", "0", "100");
    }

    @Test
    public void testUnlinkModel() {
        modelService.unlinkModel("nmodel_basic_inner", "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel nDataModel = dataModelManager.getDataModelDesc("nmodel_basic_inner");
        Assert.assertEquals(ManagementType.MODEL_BASED, nDataModel.getManagementType());
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Model nmodel_basic_inner is model based, can not unlink it!");
        modelService.unlinkModel("nmodel_basic_inner", "default");
    }

    @Test
    public void testGetCCUsage() {
        ComputedColumnUsageResponse usages = modelService.getComputedColumnUsages("default");
        Assert.assertTrue(usages.getUsageMap().get("TEST_KYLIN_FACT.DEAL_AMOUNT").getModels().size() == 2);
        Assert.assertTrue(usages.getUsageMap().get("TEST_KYLIN_FACT.SELLER_COUNTRY_ABBR") == null);
        Assert.assertTrue(
                usages.getUsageMap().get("TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR").getModels().size() == 1);
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testAddSameNameDiffExprNormal() throws IOException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT")
                        && ccException.getConflictingModel().equals("nmodel_basic_inner")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.DEAL_AMOUNT is already used in model nmodel_basic_inner,"
                                        + " you should apply the same expression as ' TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT ' here,"
                                        + " or use a different computed column name.");

            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();

        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_basic", "default", true, null, null,
                "", false);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("expression");
        field.setAccessible(true);
        field.set(deserialized.getComputedColumnDescs().get(0), "1+1");
        modelService.getDataModelManager("default").updateDataModelDesc(deserialized);
        // TODO should use modelService.updateModelAndDesc("default", deserialized);
    }

    @Test
    public void testFailureModelUpdateDueToComputedColumnConflict2()
            throws IOException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("There is already a column named CAL_DT on table DEFAULT.TEST_KYLIN_FACT,"
                + " please change your computed column name");

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_basic", "default", true, null, null,
                "", false);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("columnName");
        field.setAccessible(true);
        field.set(deserialized.getComputedColumnDescs().get(0), "cal_dt");
        modelService.getDataModelManager("default").updateDataModelDesc(deserialized);
        // TODO should use modelService.updateModelAndDesc("default", deserialized);
    }

    /*
     * start to test with model new_ci_left_join_model, which is structurely same as ci_left_join_model,
     * but with different alias
     */

    @Test
    public void testCCExpressionNotReferingHostAlias1() throws IOException {
        expectedEx.expect(BadModelException.class);
        expectedEx.expectMessage(
                "A computed column should be defined on root fact table if its expression is not referring its hosting alias table,"
                        + " cc: BUYER_ACCOUNT.LEFTJOIN_SELLER_COUNTRY_ABBR");
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        //replace last cc's host alias
        contents = StringUtils.reverse(
                StringUtils.reverse(contents).replaceFirst(StringUtils.reverse("\"tableAlias\": \"TEST_KYLIN_FACT\""),
                        StringUtils.reverse("\"tableAlias\": \"BUYER_ACCOUNT\"")));

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testCCExpressionNotReferingHostAlias2() throws IOException {
        expectedEx.expect(BadModelException.class);
        expectedEx.expectMessage(
                "A computed column should be defined on root fact table if its expression is not referring its hosting alias table,"
                        + " cc: BUYER_ACCOUNT.DEAL_AMOUNT");
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        //replace first cc's host alias
        String str = "\"columnName\": \"DEAL_AMOUNT\",";
        int index = contents.indexOf(str);
        contents = contents.substring(0, str.length() + index) + "\"tableAlias\": \"BUYER_ACCOUNT\","
                + contents.substring(str.length() + index);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprSameNameNormal() throws IOException {

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprSameNameOnDifferentAliasTable() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;

                return ccException.getCauseType().equals(BadModelException.CauseType.WRONG_POSITION_DUE_TO_NAME)
                        && ccException.getAdvise().equals("TEST_KYLIN_FACT")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("SELLER_ACCOUNT.LEFTJOIN_SELLER_COUNTRY_ABBR")
                        && ccException.getMessage().equals(
                                "Computed column LEFTJOIN_SELLER_COUNTRY_ABBR is already defined in model nmodel_basic,"
                                        + " to reuse it you have to define it on alias table: TEST_KYLIN_FACT");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace(
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                        + "      \"tableAlias\": \"TEST_KYLIN_FACT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }",
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_ACCOUNT\",\n"
                        + "      \"tableAlias\": \"SELLER_ACCOUNT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprSameNameOnDifferentAliasTableCannotProvideAdvice() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.WRONG_POSITION_DUE_TO_NAME)
                        && ccException.getConflictingModel().equals("nmodel_cc_test")
                        && ccException.getBadCC().equals("TEST_ORDER.ID_PLUS_1") && ccException.getAdvise() == null
                        && ccException.getMessage().equals(
                                "Computed column ID_PLUS_1 is already defined in model nmodel_cc_test, no suggestion could be provided to reuse it");
            }
        });

        //save ut_left_join_cc_model, which is a model defining cc on lookup table
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");
        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
        val request = new ModelRequest(deserialized);
        request.setProject("default");
        modelService.createModel(request.getProject(), request);

        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_cc_test", "default", true, null, null,
                "", false);
        Assert.assertTrue(dataModelDescs.size() == 1);

        contents = contents.replaceFirst("\"type\": \"LEFT\"", "\"type\": \"INNER\"");
        contents = contents.replace("nmodel_cc_test", "nmodel_cc_test_2");

        bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testSeekAdviseOnLookTable() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getConflictingModel().equals("nmodel_cc_test")
                        && "UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)".equals(ccException.getAdvise())
                        && ccException.getBadCC().equals("BUYER_ACCOUNT.COUNTRY_UPPER")
                        && ccException.getMessage().equals(
                                "Column name for computed column BUYER_ACCOUNT.COUNTRY_UPPER is already used in model nmodel_cc_test, you should apply the same expression as ' UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY) ' here, or use a different computed column name.");
            }
        });

        //save nmodel_cc_test, which is a model defining cc on lookup table
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");
        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        val request = new ModelRequest(deserialized);
        request.setProject("default");
        modelService.createModel(request.getProject(), request);
        //TODO modelService.updateModelToResourceStore(deserialized, "default");

        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_cc_test", "default", true, null, null,
                "", false);
        Assert.assertTrue(dataModelDescs.size() == 1);

        contents = StringUtils.reverse(StringUtils.reverse(contents).replaceFirst(
                Pattern.quote(StringUtils.reverse("\"expression\": \"UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)\",")),
                StringUtils.reverse("\"expression\": null, ")));
        contents = contents.replace("nmodel_cc_test", "nmodel_cc_test_2");

        bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    @Test
    public void testNewModelAddSameExprDiffNameOnDifferentAliasTable() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.WRONG_POSITION_DUE_TO_EXPR)
                        && ccException.getAdvise().equals("TEST_KYLIN_FACT")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("SELLER_ACCOUNT.LEFTJOIN_SELLER_COUNTRY_ABBR_2")
                        && ccException.getMessage().equals(
                                "Computed column LEFTJOIN_SELLER_COUNTRY_ABBR_2's expression is already defined in model nmodel_basic, to reuse it you have to define it on alias table: TEST_KYLIN_FACT");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace(
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                        + "      \"tableAlias\": \"TEST_KYLIN_FACT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }",
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_ACCOUNT\",\n"
                        + "      \"tableAlias\": \"SELLER_ACCOUNT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR_2\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameNameDiffExpr1() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR is already used in model nmodel_basic, you should apply the same expression as ' SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1) ' here, or use a different computed column name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)",
                "SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,2)");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameNameDiffExpr2() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic, you should apply the same expression as ' CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME) ' here, or use a different computed column name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)",
                "SUBSTR(CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME),0,1)");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprDiffName() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_EXPR_DIFF_NAME)
                        && ccException.getAdvise().equals("LEFTJOIN_BUYER_COUNTRY_ABBR")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR_2")
                        && ccException.getMessage().equals(
                                "Expression SUBSTR(BUYER_ACCOUNT.ACCOUNT_COUNTRY,0,1) in computed column TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR_2 is already defined by computed column TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR from model nmodel_basic, you should use the same column name: ' LEFTJOIN_BUYER_COUNTRY_ABBR ' .");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_BUYER_COUNTRY_ABBR_2");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameNameDiffExprModelToNonDefaultProject() throws IOException {
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)",
                "SUBSTR(CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME),0,1)");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //it's adding to non-default project, should be okay because cc conflict check is by project
        modelService.getDataModelManager("newten").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "non-default");
    }

    @Test
    public void testNewModelAddDiffNameSameExprModelToNonDefaultProject() throws IOException {
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_BUYER_COUNTRY_ABBR_2");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //it's adding to non-default project, should be okay because cc conflict check is by project
        modelService.getDataModelManager("newten").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "non-default");
    }

    @Test
    public void testCCAdviseNormalCase() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic, you should apply the same expression as ' CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME) ' here, or use a different computed column name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("\"CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)\"", "null");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    @Test
    public void testCCAdviseWithNonExistingName() throws IOException {

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("No advice could be provided");

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace(" \"columnName\": \"LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME\",",
                " \"columnName\": \"LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME_2\",");
        contents = contents.replace(" \"column\": \"TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME\"",
                " \"column\": \"TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME_2\"");
        contents = contents.replace("\"CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)\"", "null");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);
    }

    @Test
    public void testCCNameCheck() {
        ModelService.checkCCName("cc_1");
        try {
            // HIVE
            ModelService.checkCCName("LOCAL");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("The computed column's name:LOCAL is a sql keyword, please choose another name.",
                    e.getMessage());
        }

        try {
            // CALCITE
            ModelService.checkCCName("MSCK");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("The computed column's name:MSCK is a sql keyword, please choose another name.",
                    e.getMessage());
        }

    }

    @Test
    public void testCCAdviseUnmatchingSubgraph() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise() == null && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic, you should apply the same expression like ' CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME) ' here, or use a different computed column name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("\"CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)\"", "null");

        //replace last join's type, which is for SELLER_ACCOUNT
        contents = StringUtils.reverse(StringUtils.reverse(contents)
                .replaceFirst(StringUtils.reverse("\"type\": \"LEFT\""), StringUtils.reverse("\"type\": \"INNER\"")));

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    @Test
    public void testCCAdviseMatchingSubgraph() throws IOException {
        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME")

                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic, you should apply the same expression as ' CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME) ' here, or use a different computed column name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("\"CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME)\"", "null");

        //replace last join's type, which is for SELLER_ACCOUNT
        contents = StringUtils.reverse(StringUtils.reverse(contents)
                .replaceFirst(StringUtils.reverse("\"type\": \"LEFT\""), StringUtils.reverse("\"type\": \"INNER\"")));

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    /*
     * now test conflict within a model
     */

    @Test
    public void testSameNameSameExprInOneModelNormal() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, at least two computed columns share the same column name: DEAL_AMOUNT, please use different column name");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"columnName\": \"DEAL_AMOUNT\",\n" + "      \"expression\": \"PRICE * ITEM_COUNT\",\n"
                + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
        contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testSameNameOnDifferentAliasTableInOneModel() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, at least two computed columns share the same column name: DEAL_AMOUNT, please use different column name");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = "  {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_ACCOUNT\",\n"
                + "      \"tableAlias\": \"BUYER_ACCOUNT\",\n" + "      \"columnName\": \"DEAL_AMOUNT\",\n"
                + "      \"expression\": \"BUYER_ACCOUNT.ACCOUNT_ID\",\n" + "      \"datatype\": \"bigint\",\n"
                + "      \"comment\": \"bla bla\"\n" + "    },";
        contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testDiffNameSameExprInOneModelNormal() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, computed column TEST_KYLIN_FACT.DEAL_AMOUNT share same expression as TEST_KYLIN_FACT.DEAL_AMOUNT_2, please remove one");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"columnName\": \"DEAL_AMOUNT_2\",\n" + "      \"expression\": \"PRICE * ITEM_COUNT\",\n"
                + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
        contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    //compared with testDiffNameSameExprInOneModelNormal, expression is normalized
    public void testDiffNameSameExprInOneModelWithSlightlyDifferentExpression() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, computed column TEST_KYLIN_FACT.DEAL_AMOUNT share same expression as TEST_KYLIN_FACT.DEAL_AMOUNT_2, please remove one");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"columnName\": \"DEAL_AMOUNT_2\",\n"
                + "      \"expression\": \"TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT\",\n"
                + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
        contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    /**
     * start to the side effect of bad model
     */

    /**
     * if a bad model is detected, it should not affect the existing table desc
     * <p>
     * same bad model as testDiffNameSameExprInOneModelWithSlightlyDifferentExpression
     */
    @Test
    public void testCreateBadModelWontAffectTableDesc() throws IOException {

        try {
            Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
            String contents = StringUtils.join(Files.readAllLines(
                    new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                    Charset.defaultCharset()), "\n");

            String str = "\"computed_columns\": [";
            int i = contents.indexOf(str) + str.length();
            String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                    + "      \"columnName\": \"DEAL_AMOUNT_2\",\n"
                    + "      \"expression\": \"TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT\",\n"
                    + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
            contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

            InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
            NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
            modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
            //TODO modelService.updateModelToResourceStore(deserialized, "default");
        } catch (BadModelException e) {
            modelService.getTableManager("default").resetProjectSpecificTableDesc();
            TableDesc aDefault = modelService.getTableManager("default").getTableDesc("DEFAULT.TEST_KYLIN_FACT");
            Collection<String> allColumnNames = Collections2.transform(Arrays.asList(aDefault.getColumns()),
                    new Function<ColumnDesc, String>() {
                        @Nullable
                        @Override
                        public String apply(@Nullable ColumnDesc columnDesc) {
                            return columnDesc.getName();
                        }
                    });
            Assert.assertTrue(!allColumnNames.contains("DEAL_AMOUNT_2"));
        }
    }

    @Test
    /**
     * testSeekAdviseOnLookTable
     */
    public void testSeekAdviceWontAffectTableDesc() throws IOException {

        try {
            //save nmodel_cc_test, which is a model defining cc on lookup table
            Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
            String contents = StringUtils.join(Files.readAllLines(
                    new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                    Charset.defaultCharset()), "\n");
            InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
            NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
            val request = new ModelRequest(deserialized);
            request.setProject("default");
            modelService.createModel(request.getProject(), request);
            //TODO modelService.updateModelToResourceStore(deserialized, "default");

            List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_cc_test", "default", true, null,
                    null, "", false);
            Assert.assertTrue(dataModelDescs.size() == 1);

            contents = StringUtils.reverse(StringUtils.reverse(contents).replaceFirst(
                    Pattern.quote(StringUtils.reverse("\"expression\": \"UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)\",")),
                    StringUtils.reverse("\"expression\": null, ")));
            contents = contents.replace("nmodel_cc_test", "nmodel_cc_test_2");

            bais = IOUtils.toInputStream(contents, Charset.defaultCharset());

            deserialized = serializer.deserialize(new DataInputStream(bais));
            deserialized.setSeekingCCAdvice(true);

            modelService.checkComputedColumn(deserialized, "default", null);

        } catch (BadModelException e) {
            modelService.getTableManager("default").resetProjectSpecificTableDesc();
            TableDesc aDefault = modelService.getTableManager("default").getTableDesc("DEFAULT.TEST_ACCOUNT");
            Assert.assertEquals(5, aDefault.getColumns().length);
        }
    }

    @Test
    public void testPreProcessBeforeModelSave() throws IOException {
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");
        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        NDataModel updated = NDataModel.getCopyOf(deserialized);
        List<ComputedColumnDesc> newCCs1 = Lists.newArrayList(deserialized.getComputedColumnDescs());
        ComputedColumnDesc ccDesc1 = new ComputedColumnDesc();
        ccDesc1.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc1.setColumnName("CC1");
        ccDesc1.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 1");
        ccDesc1.setDatatype("decimal");
        newCCs1.add(ccDesc1);
        updated.setComputedColumnDescs(newCCs1);
        List<ComputedColumnDesc> newCCs2 = Lists.newArrayList(deserialized.getComputedColumnDescs());
        ComputedColumnDesc ccDesc2 = new ComputedColumnDesc();
        ccDesc2.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc2.setColumnName("CC2");
        ccDesc2.setExpression("CC1 * 2");
        ccDesc2.setDatatype("decimal");
        newCCs2.add(ccDesc1);
        newCCs2.add(ccDesc2);
        updated.setComputedColumnDescs(newCCs2);

        Assert.assertEquals("CC1 * 2", ccDesc2.getInnerExpression());
        modelService.preProcessBeforeModelSave(updated, "default");
        Assert.assertEquals("(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 1) * 2 ",
                ccDesc2.getInnerExpression());

        ccDesc1.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 2");
        modelService.preProcessBeforeModelSave(updated, "default");
        Assert.assertEquals("(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 2) * 2 ",
                ccDesc2.getInnerExpression());

        ccDesc2.setExpression("CC1 * 3");
        modelService.preProcessBeforeModelSave(updated, "default");
        Assert.assertEquals("(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 2) * 3 ",
                ccDesc2.getInnerExpression());
    }

    @Test
    @Ignore("will create cube with model")
    public void testBuildSegmentsManuallyException1() {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "match")
                .getDataModelDesc("match");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setName("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setManagementType(ManagementType.MODEL_BASED);
        modelRequest.setLastModified(0L);
        modelRequest.setProject("match");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not build segments, please define table index or aggregate index first!");
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelService.buildSegmentsManually("match", "new_model", "0", "100");
    }

    @Test
    public void testBuildSegmentsManuallyException2() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("nmodel_full_measure_test");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Segments to build overlaps built or building segment(from 0 to 9223372036854775807), please select new data range and try again!");
        modelService.buildSegmentsManually("default", "nmodel_full_measure_test", "0", "100");
    }

    @Test
    public void testBuildSegmentsManually() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("nmodel_basic");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("ncube_basic");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getName());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        modelService.buildSegmentsManually("default", "nmodel_basic", "0", "100");
        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        modelDesc = modelManager.getDataModelDesc("nmodel_basic");
        modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate);

        val events = eventDao.getEvents();
        events.sort(Comparator.comparing(Event::getCreateTimeNanosecond));
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);
    }

    @Test
    public void testUpdateModelDataCheckDesc() {
        modelService.updateModelDataCheckDesc("default", "nmodel_basic", 7, 10, 2);
        final NDataModel dataModel = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("nmodel_basic");

        final DataCheckDesc dataCheckDesc = dataModel.getDataCheckDesc();
        Assert.assertEquals(7, dataCheckDesc.getCheckOptions());
        Assert.assertEquals(10, dataCheckDesc.getFaultThreshold());
        Assert.assertEquals(2, dataCheckDesc.getFaultActions());
    }

    @Test
    public void testGetAffectedModelsByToogleTableType() {
        val response = modelService.getAffectedModelsByToggleTableType("DEFAULT.TEST_KYLIN_FACT", "default", true);
        Assert.assertEquals(4, response.getModels().size());
        Assert.assertEquals(5633024L, response.getByteSize());
    }

    @Test
    public void testSetIncrementing_LimitedFactTable_exception() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelManager.getDataModelDesc("nmodel_basic");
        val joinTableDesc = new JoinTableDesc();
        joinTableDesc.setTable("DEFAULT.TEST_KYLIN_FACT");
        model.setJoinTables(Lists.newArrayList(joinTableDesc));
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_KYLIN_FACT' incrementing loading, due to another incrementing loading table existed in model 'nmodel_basic'!");
        modelService.checkSingleIncrementingLoadingTable("default", "DEFAULT.TEST_KYLIN_FACT");
    }
    
    @Test
    public void testGetModelInfoByModel() throws IOException {
        val result1 = new QueryTimesResponse();
        result1.setModel("nmodel_basic");
        result1.setQueryTimes(10);
        val result2 = new QueryTimesResponse();
        result2.setModel("nmodel_full_measure_test");
        result2.setQueryTimes(10);
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(result1, result2)).when(queryHistoryDAO).getQueryTimesResponseBySql(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(),
                Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(modelService).getQueryHistoryDao(Mockito.anyString());
        val modelInfo = modelService.getModelInfo("*", "nmodel_basic", "default", 0, 0);
        Assert.assertEquals(1, modelInfo.size());
        Assert.assertEquals(10, modelInfo.get(0).getQueryTimes());
        Assert.assertEquals(3380224, modelInfo.get(0).getModelStorageSize());
        val modelInfo2 = modelService.getModelInfo("*", "nmodel_full_measure_test", "default", 0, 0);
        Assert.assertEquals(1, modelInfo2.size());
        Assert.assertEquals(10, modelInfo2.get(0).getQueryTimes());
        Assert.assertEquals(0, modelInfo2.get(0).getModelStorageSize());
    }

    @Test
    public void testGetModelInfoByProject() throws IOException {
        val result1 = new QueryTimesResponse();
        result1.setModel("nmodel_basic");
        result1.setQueryTimes(10);
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(result1)).when(queryHistoryDAO).getQueryTimesResponseBySql(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(),
                Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(modelService).getQueryHistoryDao(Mockito.anyString());
        val modelInfo = modelService.getModelInfo("*", "*", "default", 0, 0);
        Assert.assertEquals(6, modelInfo.size());
        Assert.assertEquals(10, modelInfo.get(1).getQueryTimes());
        Assert.assertEquals(3380224, modelInfo.get(1).getModelStorageSize());
    }

    @Test
    public void testGetAllModelInfo() throws IOException {
        val result1 = new QueryTimesResponse();
        result1.setModel("nmodel_basic");
        result1.setQueryTimes(10);
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(result1)).when(queryHistoryDAO).getQueryTimesResponseBySql(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(),
                Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(modelService).getQueryHistoryDao(Mockito.anyString());
        val modelInfo = modelService.getModelInfo("*", "*", "*", 0, 0);
        Assert.assertEquals(9, modelInfo.size());
    }

    @Test
    public void testGetModelInfo_ProjectEmpty_exception() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Project name can not be empty when model is selected!");
        modelService.getModelInfo("*", "nmodel_basic", "*", 0, 0);
    }

    @Test
    public void testGetModelInfo_ModelNotExist_exception() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model 'nmodel_basic2222' does not exist!");
        modelService.getModelInfo("*", "nmodel_basic2222", "default", 0, 0);
    }

    private List<AbstractExecutable> mockJobs() {
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedTestExecutable job1 = new SucceedTestExecutable();
        job1.setProject("default");
        job1.initConfig(KylinConfig.getInstanceFromEnv());
        job1.setName("sparkjob1");
        job1.setTargetModel("nmodel_basic_inner");
        SucceedTestExecutable job2 = new SucceedTestExecutable();
        job2.setProject("default");
        job2.initConfig(KylinConfig.getInstanceFromEnv());
        job2.setName("sparkjob2");
        job2.setTargetModel("model2");
        SucceedTestExecutable job3 = new SucceedTestExecutable();
        job3.setProject("default");
        job3.initConfig(KylinConfig.getInstanceFromEnv());
        job3.setName("sparkjob3");
        job3.setTargetModel("model3");
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);
        return jobs;
    }
}
