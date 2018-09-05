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

import java.util.List;

import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

/**
 * @author zy
 */
public class ModelServiceTest extends NLocalFileMetadataTestCase {
    private final ModelService modelService = new ModelService();
    private final TableService tableService = new TableService();

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();

    }

    @Before
    public void setup() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        ReflectionTestUtils.setField(modelService, "aclEvaluate", Mockito.mock(AclEvaluate.class));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", Mockito.mock(AclEvaluate.class));
    }

    @AfterClass
    public static void tearDown() {
        cleanAfterClass();
    }

    @Test
    public void testGetModels() throws Exception {

        List<NDataModel> models = modelService.getModels("", "default", false);
        Assert.assertEquals(models.size(), 4);

    }

    @Test
    public void testGetSegments() throws Exception {

        Segments<NDataSegment> segments = modelService.getSegments("ncube_basic", "default", 1, Long.MAX_VALUE - 1);
        Assert.assertEquals(segments.size(), 0);

    }

    @Test
    public void testGetAggIndexs() throws Exception {

        List<NCuboidDesc> indexs = modelService.getAggIndexs("nmodel_basic", "default");
        Assert.assertEquals(indexs.size(), 7);

    }

    @Test
    public void testIsTableInModel() throws Exception {
        TableDesc tableDesc = tableService.getTableDescByName("DEFAULT.TEST_KYLIN_FACT", false, "default");
        boolean result = modelService.isTableInModel(tableDesc, "default");
        Assert.assertTrue(result);

    }

    @Test
    public void testGetTableIndexs() throws Exception {

        List<NCuboidDesc> indexs = modelService.getTableIndexs("nmodel_basic", "default");
        Assert.assertEquals(indexs.size(), 3);

    }

    @Test
    public void testGetCuboidDescs() throws Exception {

        List<NCuboidDesc> cuboids = modelService.getCuboidDescs("nmodel_basic", "default");
        Assert.assertTrue(cuboids.size() > 0);
    }

    @Test
    public void testGetModelJson() throws Exception {

        String json = modelService.getModelJson("nmodel_basic", "default");
        Assert.assertTrue(!StringUtils.isEmpty(json));
    }

    @Test
    public void testGetCuboidById() throws Exception {

        NCuboidDesc cuboid = modelService.getCuboidById("nmodel_basic", "default", 0L);
        Assert.assertTrue(cuboid.getId() == 0L);
    }

    @Test
    public void testGetModelRelations() throws Exception {

        List<NForestSpanningTree> relations = modelService.getModelRelations("nmodel_basic", "default");
        Assert.assertTrue(relations.size() > 0);
    }
}
