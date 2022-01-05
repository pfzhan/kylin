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

package org.apache.kylin.cube.gridtable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.gridtable.NLayoutToGridTableMapping;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GridTableMappingTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHandlerCountReplace() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.replace-count-column-with-count-star", "false");
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan cube = mgr.getIndexPlanByModelAlias("nmodel_basic");
        IndexEntity first = Iterables.getFirst(cube.getAllIndexes(), null);
        LayoutEntity cuboidLayout = first.getLastLayout();
        NLayoutToGridTableMapping gridTableMapping = new NLayoutToGridTableMapping(cuboidLayout);
        FunctionDesc functionDesc = new FunctionDesc();
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setType("field");
        parameterDesc.setValue("CUSTOMER_ID");
        functionDesc.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc.setExpression("COUNT");
        functionDesc.setReturnType("bigint");
        Integer measureIndex = gridTableMapping.handlerCountReplace(functionDesc);
        Assert.assertNull(measureIndex);
        FunctionDesc functionDesc1 = new FunctionDesc();
        functionDesc1.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc1.setExpression("SUM");
        functionDesc1.setReturnType("bigint");
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc1);
        Assert.assertNull(measureIndex);
        config.setProperty("kylin.query.replace-count-column-with-count-star", "true");
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc);
        Assert.assertEquals("33", measureIndex.toString());
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc1);
        Assert.assertNull(measureIndex);
    }

    @Test
    public void testGetIndexOf() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.replace-count-column-with-count-star", "false");
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan cube = mgr.getIndexPlanByModelAlias("nmodel_basic");
        IndexEntity first = Iterables.getFirst(cube.getAllIndexes(), null);
        LayoutEntity cuboidLayout = first.getLastLayout();
        NLayoutToGridTableMapping gridTableMapping = new NLayoutToGridTableMapping(cuboidLayout);
        FunctionDesc functionDesc = new FunctionDesc();
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setType("field");
        parameterDesc.setValue("CUSTOMER_ID");
        functionDesc.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc.setExpression("COUNT");
        functionDesc.setReturnType("bigint");
        Integer measureIndex = gridTableMapping.getIndexOf(functionDesc);
        Assert.assertEquals("-1", measureIndex.toString());
        FunctionDesc functionDesc1 = new FunctionDesc();
        functionDesc1.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc1.setExpression("SUM");
        functionDesc1.setReturnType("bigint");
        measureIndex = gridTableMapping.getIndexOf(functionDesc1);
        Assert.assertEquals("-1", measureIndex.toString());
        config.setProperty("kylin.query.replace-count-column-with-count-star", "true");
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc);
        Assert.assertEquals("33", measureIndex.toString());
        measureIndex = gridTableMapping.getIndexOf(functionDesc1);
        Assert.assertEquals("-1", measureIndex.toString());
    }

}