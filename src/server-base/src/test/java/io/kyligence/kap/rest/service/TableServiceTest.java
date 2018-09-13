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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import io.kyligence.kap.metadata.model.NTableDesc;
import io.kyligence.kap.metadata.model.NTableExtDesc;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import com.google.common.collect.Lists;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
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
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class TableServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ProjectService projectService = Mockito.spy(new ProjectService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();

    }

    @Before
    public void setup() throws Exception {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        ReflectionTestUtils.setField(tableService, "modelService", modelService);
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "11");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                projectInstance.getRealizationEntries(), projectInstance.getModels());
        projectService.updateProject(projectInstanceUpdate, projectInstance);
    }

    @AfterClass
    public static void tearDown() {
        cleanAfterClass();
    }

    @Test
    public void testGetTableDesc() throws Exception {

        List<TableDesc> tableDesc = tableService.getTableDesc("default", true, "");

        Assert.assertEquals(true, tableDesc.size() > 0);
        List<TableDesc> tables = tableService.getTableDesc("default", true, "DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(true, tables.get(0).getName().equals("TEST_COUNTRY"));
    }

    @Test
    public void testExtractTableMeta() throws Exception {
        String[] tables = { "DEFAULT.TEST_ACCOUNT", "DEFAULT.TEST_KYLIN_FACT" };
        List<Pair<TableDesc, TableExtDesc>> result = tableService.extractTableMeta(tables, "default", 11);
        Assert.assertEquals(true, result.size() == 2);

    }

    @Test
    public void testLoadTableToProject() throws IOException {
        List<TableDesc> tables = tableService.getTableDesc("default", true, "DEFAULT.TEST_COUNTRY");
        NTableDesc nTableDesc = new NTableDesc(tables.get(0));
        TableExtDesc tableExt = new TableExtDesc();
        tableExt.setIdentity("DEFAULT.TEST_COUNTRY");
        tableExt.updateRandomUuid();
        NTableExtDesc tableExtDesc = new NTableExtDesc(tableExt);
        String[] result = tableService.loadTableToProject(nTableDesc, tableExtDesc, "default");
        Assert.assertTrue(result.length == 1);
    }

    @Test
    public void testGetSourceDbNames() throws Exception {
        List<String> dbNames = tableService.getSourceDbNames("default", 11);
        ArrayList<String> dbs = Lists.newArrayList(dbNames);
        Assert.assertTrue(dbs.contains("DEFAULT"));
    }

    @Test
    public void testGetSourceTableNames() throws Exception {
        List<String> tableNames = tableService.getSourceTableNames("default", "DEFAULT", 11);
        Assert.assertTrue(tableNames.contains("TEST_ACCOUNT"));
    }

    @Test
    public void testNormalizeHiveTableName() {
        String tableName = tableService.normalizeHiveTableName("DEFaULT.TeST_ACCOUNT");
        Assert.assertTrue(tableName.equals("DEFAULT.TEST_ACCOUNT"));
    }

    @Test
    public void testSetFactAndSetDataRange() throws Exception {

        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT");
        List<TableDesc> tables = tableService.getTableDesc("default", false, "DEFAULT.TEST_KYLIN_FACT");
        tableService.setDataRange("default", "DEFAULT.TEST_KYLIN_FACT",
                new SegmentRange.TimePartitionedSegmentRange(1L, 1534824000000L));
        Assert.assertTrue(tables.get(0).getFact() && tables.get(0).getName().equals("TEST_KYLIN_FACT"));
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("NDataLoadingRange is related in models");
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", false, "");

        //set Account fact true and false
        tableService.setFact("DEFAULT.TEST_ACCOUNT", "default", true, "ACCOUNT_BUYER_LEVEL");
        List<TableDesc> tables2 = tableService.getTableDesc("default", false, "DEFAULT.TEST_KYLIN_FACT");
        tableService.setDataRange("default", "DEFAULT.TEST_ACCOUNT",
                new SegmentRange.TimePartitionedSegmentRange(1L, 1534824000000L));
        Assert.assertTrue(tables2.get(0).getFact() && tables2.get(0).getName().equals("DEFAULT.TEST_ACCOUNT"));
        tableService.setFact("DEFAULT.TEST_ACCOUNT", "default", false, "");
        Assert.assertTrue(!tables2.get(0).getFact() && tables2.get(0).getName().equals("DEFAULT.TEST_ACCOUNT"));
    }

    @Test
    public void testSetDateRangeException() throws IOException, PersistentException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("this table can not set date range, plz check table");
        tableService.setDataRange("default", "TEST_ACCOUNT", new SegmentRange.TimePartitionedSegmentRange(0L, 10000L));
    }

}
