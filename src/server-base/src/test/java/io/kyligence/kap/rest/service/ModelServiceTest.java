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
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

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
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.source.jdbc.H2Database;
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
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.rest.execution.SucceedTestExecutable;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.CuboidStatus;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndexEntityResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NSpanningTreeResponse;
import io.kyligence.kap.rest.response.ParameterResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SimplifiedColumnResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import lombok.val;
import lombok.var;

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
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(result1)).when(queryHistoryDAO).getQueryTimesByModel(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(modelService).getQueryHistoryDao(Mockito.anyString());
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        prjManager.updateProject(copy);
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
        Assert.assertEquals(0, model4.get(0).getStorage());
        Assert.assertEquals(0, model4.get(0).getUsage());
        List<NDataModelResponse> model5 = modelService.getModels("nmodel_full_measure_test", "default", false, "adm",
                "DISABLED", "last_modify", true);
        Assert.assertEquals(0, model5.size());

    }

    @Test
    @Ignore
    public void testGetModelsWithCC() {
        List<NDataModelResponse> models = modelService.getModels("nmodel_basic", "default", true, "", "", "", false);
        Assert.assertEquals(1, models.size());
        NDataModelResponse model = models.get(0);
        Assert.assertTrue(model.getSimpleTables().stream().map(t -> t.getColumns()).flatMap(List::stream)
                .anyMatch(SimplifiedColumnResponse::isComputedColumn));
    }

    @Test
    public void testGetSegmentsByRange() {
        Segments<NDataSegment> segments = modelService.getSegmentsByRange("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default", "0", "" + Long.MAX_VALUE);
        Assert.assertEquals(1, segments.size());
    }

    @Test
    public void testGetSegmentsResponse() {
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default", "0", "" + Long.MAX_VALUE, "start_time", false, "");
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(3380224, segments.get(0).getBytesSize());
        Assert.assertEquals("16", segments.get(0).getAdditionalInfo().get("file_count"));
        Assert.assertEquals("ONLINE", segments.get(0).getStatusToDisplay().toString());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);

        Segments<NDataSegment> segs = new Segments();
        val seg = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "0",
                "" + Long.MAX_VALUE, "start_time", false, "");
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals("LOCKED", segments.get(0).getStatusToDisplay().toString());

        seg.setStatus(SegmentStatusEnum.READY);
        segs.add(seg);
        dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToUpdateSegs(segs.toArray(new NDataSegment[segs.size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "0",
                "" + Long.MAX_VALUE, "start_time", false, "");
        Assert.assertEquals(2, segments.size());
        Assert.assertEquals("REFRESHING", segments.get(1).getStatusToDisplay().toString());

        Segments<NDataSegment> segs2 = new Segments<>();
        Segments<NDataSegment> segs3 = new Segments<>();

        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val seg2 = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        seg2.setStatus(SegmentStatusEnum.READY);
        segs3.add(seg2);
        val segToRemove = dataflow.getSegment(segments.get(1).getId());
        segs2.add(segToRemove);
        dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(segs2.toArray(new NDataSegment[segs2.size()]));
        dataflowUpdate.setToUpdateSegs(segs3.toArray(new NDataSegment[segs3.size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(0L, 20L));
        segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "0",
                "" + Long.MAX_VALUE, "start_time", false, "");
        Assert.assertEquals(3, segments.size());
        Assert.assertEquals("MERGING", segments.get(2).getStatusToDisplay().toString());
    }

    @Test
    public void testGetAggIndices() {

        List<IndexEntityResponse> indices = modelService.getAggIndices("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default");
        Assert.assertEquals(5, indices.size());
        Assert.assertTrue(indices.get(0).getId() < IndexEntity.TABLE_INDEX_START_ID);

    }

    @Test
    public void testGetTableIndices() {

        List<IndexEntityResponse> indices = modelService.getTableIndices("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default");
        Assert.assertEquals(4, indices.size());
        Assert.assertTrue(indices.get(0).getId() >= IndexEntity.TABLE_INDEX_START_ID);

    }

    @Test
    public void testGetCuboidDescs() {

        List<IndexEntity> cuboids = modelService.getIndexEntities("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        Assert.assertEquals(9, cuboids.size());
    }

    @Test
    public void testGetCuboidById_AVAILABLE() {
        IndexEntityResponse cuboid = modelService.getCuboidById("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", 0L);
        Assert.assertEquals(0L, cuboid.getId());
        Assert.assertEquals(CuboidStatus.AVAILABLE, cuboid.getStatus());
        Assert.assertEquals(252928L, cuboid.getStorageSize());
    }

    @Test
    public void testGetCuboidById_NoSegments_EMPTYStatus() {
        IndexEntityResponse cuboid = modelService.getCuboidById("82fa7671-a935-45f5-8779-85703601f49a", "default",
                130000L);
        Assert.assertEquals(130000L, cuboid.getId());
        Assert.assertEquals(CuboidStatus.EMPTY, cuboid.getStatus());
        Assert.assertEquals(0L, cuboid.getStorageSize());
        Assert.assertEquals(0L, cuboid.getStartTime());
        Assert.assertEquals(0L, cuboid.getEndTime());
    }

    @Test
    public void testGetModelJson() throws IOException {
        String modelJson = modelService.getModelJson("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        Assert.assertTrue(JsonUtil.readValue(modelJson, NDataModel.class).getUuid()
                .equals("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
    }

    @Test
    public void testGetModelRelations() {
        List<NSpanningTree> relations = modelService.getModelRelations("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default");
        Assert.assertEquals(1, relations.size());

        relations = modelService.getModelRelations("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(6, relations.get(0).getBuildLevel());
        Assert.assertThat(
                relations.get(0).getCuboidsByLayer().stream().map(Collection::size).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList(1, 5, 4, 4, 1, 1, 1)));

        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        indePlanManager.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", copyForWrite -> {
            val rule = new NRuleBasedIndex();
            rule.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            rule.setMeasures(Lists.newArrayList(100001, 100002));
            try {
                val aggGroup = JsonUtil.readValue("{\n" + "        \"includes\": [1, 2, 3],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [],\n"
                        + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [],\n"
                        + "          \"dim_cap\": 1\n" + "        }\n" + "      }", NAggregationGroup.class);
                rule.setAggregationGroups(Lists.newArrayList(aggGroup));
                copyForWrite.setRuleBasedIndex(rule);
            } catch (IOException ignore) {
            }
        });
        relations = modelService.getModelRelations("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(4, relations.get(0).getBuildLevel());
    }

    @Test
    public void testGetSimplifiedModelRelations() {
        List<NSpanningTreeResponse> relations = modelService
                .getSimplifiedModelRelations("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(1, relations.get(0).getRoots().size());
        Assert.assertEquals(5, relations.get(0).getNodesMap().size());
    }

    @Test
    public void testDropModelExceptionName() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.dropModel("nmodel_basic2222", "default");
    }

    @Test
    public void testDropModelPass() {
        modelService.dropModel("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
        List<NDataModelResponse> models = modelService.getModels("test_encoding", "default", true, "", "",
                "last_modify", true);
        Assert.assertTrue(CollectionUtils.isEmpty(models));

    }

    @Test
    public void testPurgeModelManually() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        NDataModel modelUpdate = modelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        modelService.purgeModelManually("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
        List<NDataSegment> segments = modelService.getSegmentsByRange("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default",
                "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(CollectionUtils.isEmpty(segments));
    }

    @Test
    public void testPurgeModelManually_TableOriented_Exception() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        NDataModel modelUpdate = modelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model 'test_encoding' is table oriented, can not purge the model!");
        modelService.purgeModelManually("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
    }

    @Test
    public void testGetAffectedSegmentsResponse_NoSegments_Exception() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("No segments to refresh, please select new range and try again!");
        List<NDataSegment> segments = modelService.getSegmentsByRange("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default",
                "0", "" + Long.MAX_VALUE);
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
        modelService.cloneModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "nmodel_basic_inner", "default");
    }

    @Test
    public void testCloneModelExceptionName() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.cloneModel("nmodel_basic2222", "nmodel_basic_inner222", "default");
    }

    @Test
    public void testCloneModel() {
        modelService.cloneModel("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "test_encoding_new", "default");
        List<NDataModelResponse> models = modelService.getModels("", "default", true, "", "", "last_modify", true);
        Assert.assertEquals(7, models.size());
    }

    @Test
    public void testRenameModel() {
        modelService.renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "new_name");
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
        modelService.renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "nmodel_basic_inner");
    }

    @Test
    public void testUpdateDataModelStatus() {
        modelService.updateDataModelStatus("cb596712-3a09-46f8-aea1-988b43fe9b6c", "default", "OFFLINE");
        List<NDataModelResponse> models = modelService.getModels("nmodel_full_measure_test", "default", true, "", "",
                "last_modify", true);
        Assert.assertTrue(models.get(0).getUuid().equals("cb596712-3a09-46f8-aea1-988b43fe9b6c")
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
        modelService.updateDataModelStatus("741ca86a-1f13-46da-a59f-95fb68615e3a", "default", "ONLINE");
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
        NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .createDataLoadingRange(dataLoadingRange);
        modelService.updateDataModelStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "ONLINE");
        modelService.updateDataModelStatus("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default", "ONLINE");
    }

    @Test
    public void testGetSegmentRangeByModel() {
        SegmentRange segmentRange = modelService.getSegmentRangeByModel("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "0", "2322442");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange);
        SegmentRange segmentRange2 = modelService.getSegmentRangeByModel("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
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
        val modelUpdate = modelManager
                .copyForWrite(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        List<RelatedModelResponse> models = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT", "");
        Assert.assertEquals(3, models.size());
        val modelUpdate2 = modelManager
                .copyForWrite(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
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
        val result = modelService.getModelsUsingTable("DEFAULT.TEST_KYLIN_FACT", "default");
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
        thrown.expectMessage("Can not refresh, please try again and confirm affected storage!");
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
        NDataModel dataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not remove segment (ID:" + dataSegment.getId()
                + "), because this segment overlaps building segments!");
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment.getId() });
    }

    @Test
    public void testDeleteSegmentById_TableOrientedModel_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model 'nmodel_basic_inner' is table oriented, can not remove segments manually!");
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment.getId() });
    }

    @Test
    public void testDeleteSegmentById_SegmentToRefreshOverlapsBuilding_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);

        start = SegmentRange.dateToLong("2010-01-02");
        end = SegmentRange.dateToLong("2010-01-03");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dataSegment2 = dataflowManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        eventDao.deleteAllEvents();
        //refresh normally
        modelService.refreshSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment2.getId() });
        List<Event> events = eventDao.getEvents();
        // TODO check other events
        //        Assert.assertEquals(1, events.size());
        //        Assert.assertEquals(SegmentRange.dateToLong("2010-01-02"), events.get(0).getSegmentRange().getStart());
        //        Assert.assertEquals(SegmentRange.dateToLong("2010-01-03"), events.get(0).getSegmentRange().getEnd());

        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not remove segment (ID:" + dataSegment.getId()
                + "), because this segment overlaps building segments!");
        //refresh exception
        modelService.refreshSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment.getId() });
    }

    @Test
    public void testDeleteSegmentById_UnconsecutiveSegmentsToDelete_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel dataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
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
            df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        eventDao.deleteAllEvents();
        //remove normally
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { segments.get(0).getId() });
        List<Event> events = eventDao.getEvents();
        //2 dataflows
        val df2 = dataflowManager.getDataflow(dataModel.getUuid());
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Only consecutive segments in head or tail can be removed!");
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { segments.get(2).getId(), segments.get(3).getId() });
    }

    @Test
    public void testCreateModel_ExistedAlias_Exception() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model alias nmodel_basic already exists!");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setUuid("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setProject("default");
        modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    @Test
    public void testCreateModel_AutoMaintain_Exception() throws Exception {
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        prjManager.updateProject(copy);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setUuid("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not create model manually in SQL acceleration project!");
        modelService.createModel(modelRequest.getProject(), modelRequest);

    }

    @Test
    public void testCreateModel_PartitionIsNull() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setPartitionDesc(null);
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model", newModel.getAlias());
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dfManager.getDataflow(newModel.getUuid());
        Assert.assertEquals(1, df.getSegments().size());
        Assert.assertTrue(df.getSegments().get(0).getSegRange().isInfinite());

        modelManager.dropModel(newModel);
    }

    @Test
    public void testCreateModelAndBuildManully() throws Exception {
        setupPushdownEnv();
        testGetLatestData();
        testGetLatestDataWhenCreateModel();
        testCreateModel_PartitionNotNull();
        testBuildSegmentsManually_WithPushDown();
        testCreateModel_PartitionNotNull_WithStartAndEnd();
        testBuildSegmentsManually();
        testChangePartitionDesc();
        testChangePartitionDesc_OriginModelNoPartition();
        testChangePartitionDesc_NewModelNoPartitionColumn();
        cleanPushdownEnv();
    }

    private void testGetLatestData() throws Exception {
        ExistedDataRangeResponse response = modelService.getLatestDataRange("default", "", "", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals("1388534400000", response.getEndTime());
    }

    private void testGetLatestDataWhenCreateModel() throws Exception {
        ExistedDataRangeResponse response = modelService.getLatestDataRange("default", "DEFAULT.TEST_KYLIN_FACT", "CAL_DT", "");
        Assert.assertEquals("1388534400000", response.getEndTime());
    }

    public void testChangePartitionDesc() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.tomb)
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", modelRequest.getPartitionDesc().getPartitionDateColumn());

        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copy -> {
            copy.getPartitionDesc().setPartitionDateColumn("TRANS_ID");
        });

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", model.getPartitionDesc().getPartitionDateColumn());

        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testChangePartitionDesc_OriginModelNoPartition() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.tomb)
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", modelRequest.getPartitionDesc().getPartitionDateColumn());

        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copy -> {
            copy.setPartitionDesc(null);
        });

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertNull(model.getPartitionDesc());

        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testChangePartitionDesc_NewModelNoPartitionColumn() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.getPartitionDesc().setPartitionDateColumn("");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.tomb)
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testCreateModel_PartitionNotNull() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model", newModel.getAlias());
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dfManager.getDataflow(newModel.getUuid());
        Assert.assertEquals(1, df.getSegments().size());
        Assert.assertEquals(1325376000000L, df.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(1388534400000L, df.getSegments().get(0).getSegRange().getEnd());
        modelManager.dropModel(newModel);
    }

    public void testCreateModel_PartitionNotNull_WithStartAndEnd() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model2");
        modelRequest.setStart("0");
        modelRequest.setEnd("100");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        modelRequest.getPartitionDesc().setPartitionDateFormat("");
        Assert.assertEquals("", modelRequest.getPartitionDesc().getPartitionDateFormat());
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model2", newModel.getAlias());
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dfManager.getDataflow(newModel.getUuid());
        Assert.assertEquals(1, df.getSegments().size());
        Assert.assertEquals("yyyy-MM-dd", newModel.getPartitionDesc().getPartitionDateFormat());
        Assert.assertEquals(0L, df.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(100L, df.getSegments().get(0).getSegRange().getEnd());
        modelManager.dropModel(newModel);
    }

    @Test
    public void testCreateModelWithDefaultMeasures() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setStart("0");
        modelRequest.setEnd("100");
        modelRequest.setUuid(null);
        modelRequest.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model", newModel.getAlias());
        List<NDataModelResponse> models = modelService.getModels("new_model", "default", false, "ADMIN", "", "", false);
        Assert.assertEquals("COUNT_ALL", models.get(0).getSimplifiedMeasures().get(0).getName());
        modelManager.dropModel(newModel);
    }

    @Test
    public void testBuildSegmentsManually_TableOrientedModel_Exception() throws Exception {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Table oriented model 'nmodel_basic' can not build segments manually!");
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "0", "100");
    }

    @Test
    public void testUnlinkModel() {
        modelService.unlinkModel("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel nDataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertEquals(ManagementType.MODEL_BASED, nDataModel.getManagementType());
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Model nmodel_basic_inner is model based, can not unlink it!");
        modelService.unlinkModel("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
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
    public void testNewModelAddSameExprSameNameOnDifferentAliasTableCannotProvideAdvice() throws Exception {

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
        request.setStart("0");
        request.setEnd("100");
        request.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        request.setUuid(null);
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
    public void testSeekAdviseOnLookTable() throws Exception {

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
        request.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        request.setStart("0");
        request.setEnd("100");
        request.setUuid(UUID.randomUUID().toString());
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
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic,"
                                        + " you should apply the same expression as ' CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME) ' here,"
                                        + " or use a different computed column name.");
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
    public void testSeekAdviceWontAffectTableDesc() throws Exception {

        try {
            //save nmodel_cc_test, which is a model defining cc on lookup table
            Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
            String contents = StringUtils.join(Files.readAllLines(
                    new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                    Charset.defaultCharset()), "\n");
            InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
            NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
            val request = new ModelRequest(deserialized);
            request.setStart("0");
            request.setEnd("100");
            request.setProject("default");
            request.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
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
            deserialized.setUuid(UUID.randomUUID().toString());
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
        NDataModelManager modelManager = modelService.getDataModelManager("default");
        Serializer<NDataModel> serializer = modelManager.getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");
        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setCachedAndShared(true);
        NDataModel updated = modelManager.copyForWrite(deserialized);
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
    public void testBuildSegmentsManuallyException1() throws Exception {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "match")
                .getDataModelDesc("match");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setUuid("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setManagementType(ManagementType.MODEL_BASED);
        modelRequest.setLastModified(0L);
        modelRequest.setProject("match");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not build segments, please define table index or aggregate index first!");
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelService.buildSegmentsManually("match", "new_model", "0", "100");
    }

    public void testBuildSegmentsManually() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.getPartitionDesc().setPartitionDateFormat("");
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "0", "100");

        modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals("yyyy-MM-dd", modelDesc.getPartitionDesc().getPartitionDateFormat());

        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate);

        val events = eventDao.getEventsOrdered();
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertEquals(0L, dataflow.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(100L, dataflow.getSegments().get(0).getSegRange().getEnd());


        Assert.assertTrue(events.get(2) instanceof AddCuboidEvent);
    }

    public void testBuildSegmentsManually_WithPushDown() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        val events = eventDao.getEvents();
        events.sort(Event::compareTo);

        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertEquals(1325376000000L, dataflow.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(1388534400000L, dataflow.getSegments().get(0).getSegRange().getEnd());

    }

    @Test
    public void testBuildSegmentsManually_NoPartition_Exception() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        modelManager.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
            copyForWrite.setPartitionDesc(null);
        });

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflow = dataflowManager.updateDataflow(dataflowUpdate);
        Assert.assertEquals(0, dataflow.getSegments().size());
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertTrue(dataflow.getSegments().get(0).getSegRange().isInfinite());

    }

    @Test
    public void testBuildSegmentsManually_NoPartition_FullSegExisted() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        modelManager.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
            copyForWrite.setPartitionDesc(null);
        });

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        eventDao.deleteAllEvents();
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        val events = eventDao.getEvents();
        Assert.assertEquals(2, events.size());
        Assert.assertTrue(events.get(0) instanceof RefreshSegmentEvent
                || events.get(0) instanceof PostMergeOrRefreshSegmentEvent);
    }

    @Test
    public void testUpdateModelDataCheckDesc() {
        modelService.updateModelDataCheckDesc("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 7, 10, 2);
        final NDataModel dataModel = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

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
    @Ignore
    public void testSetIncrementing_LimitedFactTable_exception() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val joinTableDesc = new JoinTableDesc();
        joinTableDesc.setTable("DEFAULT.TEST_KYLIN_FACT");
        model.setJoinTables(Lists.newArrayList(joinTableDesc));
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_KYLIN_FACT' incremental loading, due to another incremental loading table existed in model 'nmodel_basic'!");
        modelService.checkSingleIncrementingLoadingTable("default", "DEFAULT.TEST_KYLIN_FACT");
    }

    @Test
    public void testGetModelInfoByModel() throws IOException {
        val projects = Lists.newArrayList("default");
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        val result2 = new QueryTimesResponse();
        result2.setModel("cb596712-3a09-46f8-aea1-988b43fe9b6c");
        result2.setQueryTimes(10);
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(result1, result2)).when(queryHistoryDAO).getQueryTimesByModel(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(modelService).getQueryHistoryDao(Mockito.anyString());
        val modelInfo = modelService.getModelInfo("*", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", projects, 0, 0);
        Assert.assertEquals(1, modelInfo.size());
        Assert.assertEquals(10, modelInfo.get(0).getQueryTimes());
        Assert.assertEquals(3380224, modelInfo.get(0).getModelStorageSize());
        val modelInfo2 = modelService.getModelInfo("*", "cb596712-3a09-46f8-aea1-988b43fe9b6c", projects, 0, 0);
        Assert.assertEquals(1, modelInfo2.size());
        Assert.assertEquals(10, modelInfo2.get(0).getQueryTimes());
        Assert.assertEquals(0, modelInfo2.get(0).getModelStorageSize());
    }


    @Test
    public void testGetModelInfoByModel_ProjectNotSpecifiedOnly() throws IOException {
        val projects = Lists.newArrayList("default", "demo");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Only one project name should be specified while model is specified!");
        modelService.getModelInfo("*", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", projects, 0, 0);
    }

    @Test
    public void testGetModelInfoByProject() throws IOException {

        List<String> projects = Lists.newArrayList("default");

        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(result1)).when(queryHistoryDAO).getQueryTimesByModel(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(modelService).getQueryHistoryDao(Mockito.anyString());
        val modelInfo = modelService.getModelInfo("*", "*", projects, 0, 0);
        Assert.assertEquals(6, modelInfo.size());
        Assert.assertEquals(10, modelInfo.get(2).getQueryTimes());
        Assert.assertEquals(3380224, modelInfo.get(2).getModelStorageSize());
    }

    @Test
    @Ignore
    public void testGetAllModelInfo() throws IOException {
        List<String> projects = Lists.newArrayList();

        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(result1)).when(queryHistoryDAO).getQueryTimesByModel(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(modelService).getQueryHistoryDao(Mockito.anyString());
        val modelInfo = modelService.getModelInfo("*", "*", projects, 0, 0);
        Assert.assertEquals(8, modelInfo.size());
    }

    @Test
    public void testGetModelInfo_ProjectEmpty_exception() throws IOException {
        List<String> projects = Lists.newArrayList();

        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Only one project name should be specified while model is specified!");
        modelService.getModelInfo("*", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", projects, 0, 0);
    }

    @Test
    public void testGetModelInfo_ModelNotExist_exception() throws IOException {
        List<String> projects = Lists.newArrayList("default");

        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model 'nmodel_basic2222' does not exist!");
        modelService.getModelInfo("*", "nmodel_basic2222", projects, 0, 0);
    }

    @Test
    public void testUpdateAndGetModelConfig() {
        val project = "default";
        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelConfigRequest = new ModelConfigRequest();
        modelConfigRequest.setProject(project);
        modelConfigRequest.setAutoMergeEnabled(false);
        modelConfigRequest.setAutoMergeTimeRanges(Lists.newArrayList(AutoMergeTimeEnum.WEEK));
        modelService.updateModelConfig(project, model, modelConfigRequest);

        var modelConfigResponses = modelService.getModelConfig(project, null);
        modelConfigResponses.forEach(modelConfigResponse -> {
            if (modelConfigResponse.getModel().equals(model)) {
                Assert.assertEquals(false, modelConfigResponse.getAutoMergeEnabled());
                Assert.assertEquals(1, modelConfigResponse.getAutoMergeTimeRanges().size());
            }
        });

        // get model config by fuzzy matching model alias
        modelConfigResponses = modelService.getModelConfig(project, "nmodel");
        Assert.assertEquals(3, modelConfigResponses.size());
        modelConfigResponses.forEach(modelConfigResponse -> {
            Assert.assertTrue(modelConfigResponse.getAlias().contains("nmodel"));
        });
    }

    private List<AbstractExecutable> mockJobs() {
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedTestExecutable job1 = new SucceedTestExecutable();
        job1.setProject("default");
        job1.initConfig(KylinConfig.getInstanceFromEnv());
        job1.setName("sparkjob1");
        job1.setTargetModel("741ca86a-1f13-46da-a59f-95fb68615e3a");
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

    private void setupPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");
    }

    private void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa",
                "");
        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
    }

    @Test
    public void testIllegalCreateModelRequest() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setProject("default");

        List<NDataModel.NamedColumn> namedColumns = modelRequest.getAllNamedColumns();

        // duplicate dimension names
        NDataModel.NamedColumn dimension = new NDataModel.NamedColumn();
        dimension.setId(38);
        dimension.setName("CAL_DT1");
        dimension.setAliasDotColumn("TEST_CAL_DT.CAL_DT");
        dimension.setStatus(NDataModel.ColumnStatus.DIMENSION);

        namedColumns.add(dimension);
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals("Duplicate dimension name 'CAL_DT1'.", ex.getMessage());
        }

        // invalid dimension name
        dimension.setName("CAL_DT1@!");
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals("Invalid dimension name 'CAL_DT1@!', only letters, numbers and underlines are supported.",
                    ex.getMessage());
        }

        namedColumns.remove(dimension);

        // invalid measure name
        List<SimplifiedMeasure> measures = Lists.newArrayList();
        SimplifiedMeasure measure1 = new SimplifiedMeasure();
        measure1.setName("illegal_measure_name@!");
        measure1.setExpression("COUNT_DISTINCT");
        measure1.setReturnType("hllc(10)");
        ParameterResponse parameterResponse = new ParameterResponse();
        parameterResponse.setType("column");
        parameterResponse.setValue("TEST_KYLIN_FACT");
        measure1.setParameterValue(Lists.newArrayList(parameterResponse));
        measures.add(measure1);
        modelRequest.setSimplifiedMeasures(measures);

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals(
                    "Invalid measure name 'illegal_measure_name@!', only letters, numbers and underlines are supported.",
                    e.getMessage());
        }

        // duplicate measure name
        measure1.setName("count_1");

        SimplifiedMeasure measure2 = new SimplifiedMeasure();
        measure2.setName("count_1");
        measure2.setExpression("COUNT_DISTINCT");
        measure2.setReturnType("hllc(10)");
        measure2.setParameterValue(Lists.newArrayList(parameterResponse));
        measures.add(measure2);

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("Duplicate measure name 'count_1'.", e.getMessage());
        }

        // duplicate measure definitions
        measure2.setName("count_2");

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("Duplicate measure definition 'count_2'.", e.getMessage());
        }

        measures.remove(measure2);

        // duplicate join conditions
        JoinTableDesc joinTableDesc = new JoinTableDesc();
        joinTableDesc.setAlias("TEST_ACCOUNT");
        joinTableDesc.setTable("DEFAULT.TEST_ACCOUNT");
        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setType("INNER");
        joinDesc.setPrimaryKey(new String[] { "TEST_ACCOUNT.ACCOUNT_ID", "TEST_ACCOUNT.ACCOUNT_ID" });
        joinDesc.setForeignKey(new String[] { "TEST_KYLIN_FACT.SELLER_ID", "TEST_KYLIN_FACT.SELLER_ID" });

        joinTableDesc.setJoin(joinDesc);
        modelRequest.setJoinTables(Lists.newArrayList(joinTableDesc));

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("Duplicate join condition 'TEST_ACCOUNT.ACCOUNT_ID' and 'TEST_KYLIN_FACT.SELLER_ID'.",
                    e.getMessage());
        }
    }

    @Test
    public void testReloadModel() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelManager.getDataModelDesc(modelId);
        List<String> models = Lists.newArrayList();
        models.add(modelId);
        modelService.reloadModels(models, project);
        val model2 = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(modelId, model2.getId());
        Assert.assertEquals(model, model2);
    }

    @Test
    public void testBuildIndexManually_TableOriented_exception() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Table oriented model 'all_fixed_length' can not build indices manually!");
        modelService.buildIndicesManually(modelId, project);
    }

    @Test
    public void testBuildIndexManually() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        modelService.buildIndicesManually(modelId, project);
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val events = eventDao.getEventsOrdered();
        Assert.assertEquals(2, events.size());
        Assert.assertTrue(events.get(0) instanceof AddCuboidEvent);

    }
}
