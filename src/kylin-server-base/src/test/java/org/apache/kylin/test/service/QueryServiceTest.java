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

package org.apache.kylin.test.service;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * @author xduo
 */
public class QueryServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("queryService")
    QueryService queryService;

    @BeforeClass
    public static void beforeClass() {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        staticCleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException, SQLException {
        Assert.assertNotNull(queryService.getConfig());
        Assert.assertNotNull(queryService.getConfig());
        Assert.assertNotNull(queryService.getDataModelManager(ProjectInstance.DEFAULT_PROJECT_NAME));
        Assert.assertNotNull(QueryConnection.getConnection(ProjectInstance.DEFAULT_PROJECT_NAME));

        SQLRequest request = new SQLRequest();
        request.setSql("select * from test_table");
        request.setAcceptPartial(true);
        SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        queryService.logQuery(request, response);
    }

    @Test
    public void testCreateTableToWith() {
        String create_table1 = " create table tableId as select * from some_table1;";
        String create_table2 = "CREATE TABLE tableId2 AS select * FROM some_table2;";
        String select_table = "select * from tableId join tableId2 on tableId.a = tableId2.b;";

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.convert-create-table-to-with", "true");
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {

            SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(create_table1);
            queryService.doQueryWithCache(request);

            request.setSql(create_table2);
            queryService.doQueryWithCache(request);

            request.setSql(select_table);
            SQLResponse response = queryService.doQueryWithCache(request, true);

            Assert.assertEquals(
                    "WITH tableId as (select * from some_table1) , tableId2 AS (select * FROM some_table2) select * from tableId join tableId2 on tableId.a = tableId2.b;",
                    response.getExceptionMessage());
        }
    }
}
