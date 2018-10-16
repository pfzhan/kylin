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

import java.io.IOException;
import java.util.List;

import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class ModelServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();

    }

    @Before
    public void setup() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

    }

    @AfterClass
    public static void tearDown() {
        cleanAfterClass();
    }

    @Test
    public void testGetModels() throws Exception {

        List<NDataModelResponse> models2 = modelService.getModels("nmodel_full_measure_test", "default", false, "", "",
                "last_modify", true);
        Assert.assertEquals(models2.size(), 1);
        List<NDataModelResponse> model3 = modelService.getModels("nmodel_full_measure_test", "default", true, "", "",
                "last_modify", true);
        Assert.assertEquals(model3.size(), 1);
        List<NDataModelResponse> model4 = modelService.getModels("nmodel_full_measure_test", "default", false, "adm",
                "", "last_modify", true);
        Assert.assertEquals(model4.size(), 1);
        List<NDataModelResponse> model5 = modelService.getModels("nmodel_full_measure_test", "default", false, "adm",
                "DISABLED", "last_modify", true);
        Assert.assertEquals(model5.size(), 0);

    }

    @Test
    public void testGetSegments() throws Exception {

        Segments<NDataSegment> segments = modelService.getSegments("nmodel_basic", "default", "0", "" + Long.MAX_VALUE);
        Assert.assertEquals(segments.size(), 2);
    }

    @Test
    public void testGetAggIndices() throws Exception {

        List<CuboidDescResponse> indices = modelService.getAggIndices("nmodel_basic", "default");
        Assert.assertEquals(indices.size(), 7);
        Assert.assertTrue(indices.get(0).getId() < NCuboidDesc.TABLE_INDEX_START_ID);

    }

    @Test
    public void testGetTableIndices() throws Exception {

        List<CuboidDescResponse> indices = modelService.getTableIndices("nmodel_basic", "default");
        Assert.assertEquals(indices.size(), 3);
        Assert.assertTrue(indices.get(0).getId() >= NCuboidDesc.TABLE_INDEX_START_ID);

    }

    @Test
    public void testGetCuboidDescs() throws Exception {

        List<NCuboidDesc> cuboids = modelService.getCuboidDescs("nmodel_basic", "default");
        Assert.assertTrue(cuboids.size() == 10);
    }

    @Test
    public void testGetCuboidById() throws Exception {

        CuboidDescResponse cuboid = modelService.getCuboidById("nmodel_basic", "default", 0L);
        Assert.assertTrue(cuboid.getId() == 0L);

        CuboidDescResponse cuboid2 = modelService.getCuboidById("nmodel_basic", "default", 1000L);
        Assert.assertTrue(cuboid2.getId() == 1000L);

    }

    @Test
    public void testGetModelJson() throws IOException {
        String modelJson = modelService.getModelJson("nmodel_basic", "default");
        Assert.assertTrue(JsonUtil.readValue(modelJson, NDataModel.class).getName().equals("nmodel_basic"));
    }

    @Test
    public void testGetModelRelations() {
        List<NForestSpanningTree> relations = modelService.getModelRelations("nmodel_basic", "default");
        Assert.assertTrue(relations.size() == 2);
    }

    @Test
    public void testDropModelException() throws IOException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("You should purge your model first before you delete it");
        modelService.dropModel("nmodel_basic_inner", "default");
    }

    @Test
    public void testDropModelExceptionName() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.dropModel("nmodel_basic2222", "default");
    }

    @Test
    public void testDropModelPass() throws IOException {
        modelService.dropModel("test_encoding", "default");
        List<NDataModelResponse> models = modelService.getModels("test_encoding", "default", true, "", "",
                "last_modify", true);
        Assert.assertTrue(CollectionUtils.isEmpty(models));

    }

    @Test
    public void testPurgeModel() throws IOException {
        modelService.purgeModel("nmodel_basic", "default");
        List<NDataSegment> segments = modelService.getSegments("nmodel_basic", "default", "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(CollectionUtils.isEmpty(segments));
    }

    @Test
    public void testPurgeModelExceptionName() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.purgeModel("nmodel_basic2222", "default");
    }

    @Test
    public void testCloneModelException() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("model alias nmodel_basic_inner already exists");
        modelService.cloneModel("nmodel_basic", "nmodel_basic_inner", "default");
    }

    @Test
    public void testCloneModelExceptionName() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.cloneModel("nmodel_basic2222", "nmodel_basic_inner222", "default");
    }

    @Test
    public void testCloneModel() throws IOException {
        modelService.cloneModel("test_encoding", "test_encoding_new", "default");
        List<NDataModelResponse> models = modelService.getModels("", "default", true, "", "", "last_modify", true);
        Assert.assertTrue(models.size() == 5);
    }

    @Test
    public void testRenameModel() throws IOException {
        modelService.renameDataModel("default", "nmodel_basic", "new_name");
        List<NDataModelResponse> models = modelService.getModels("new_name", "default", true, "", "", "last_modify",
                true);
        Assert.assertTrue(models.get(0).getAlias().equals("new_name"));
    }

    @Test
    public void testRenameModelException() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic222' not found");
        modelService.renameDataModel("default", "nmodel_basic222", "new_name");
    }

    @Test
    public void testRenameModelException2() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("model alias nmodel_basic_inner already exists");
        modelService.renameDataModel("default", "nmodel_basic", "nmodel_basic_inner");
    }

    @Test
    public void testUpdateDataModelStatus() throws IOException {
        modelService.updateDataModelStatus("nmodel_full_measure_test", "default", "DISABLED");
        List<NDataModelResponse> models = modelService.getModels("nmodel_full_measure_test", "default", true, "", "",
                "last_modify", true);
        Assert.assertTrue(models.get(0).getName().equals("nmodel_full_measure_test")
                && models.get(0).getStatus().equals(RealizationStatusEnum.DISABLED));
    }

    @Test
    public void testUpdateDataModelStatusException() throws IOException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic222' not found");
        modelService.updateDataModelStatus("nmodel_basic222", "default", "DISABLED");
    }

    @Test
    public void testGetSegmentRangeByModel() {
        SegmentRange segmentRange = modelService.getSegmentRangeByModel("default", "nmodel_basic", "0", "2322442");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange);
        SegmentRange segmentRange2 = modelService.getSegmentRangeByModel("default", "nmodel_basic", "", "");
        Assert.assertTrue(segmentRange2 instanceof SegmentRange.TimePartitionedSegmentRange && segmentRange2.getStart().equals(0L) && segmentRange2.getEnd().equals(Long.MAX_VALUE));
    }

    @Test
    public void testGetRelatedModels() throws IOException {
        List<NDataModelResponse> models = modelService.getRelateModels("default", "EDW.TEST_CAL_DT");
        Assert.assertTrue(models.size() == 2);
    }

    @Test
    public void testIsModelsUsingTable() throws IOException {
        boolean result = modelService.isModelsUsingTable("DEFAULT.TEST_KYLIN_FACT", "default");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetModelUsingTable() throws IOException {
        List<String> result = modelService.getModelsUsingTable("DEFAULT.TEST_KYLIN_FACT", "default");
        Assert.assertTrue(result.size() == 2);
    }
}
