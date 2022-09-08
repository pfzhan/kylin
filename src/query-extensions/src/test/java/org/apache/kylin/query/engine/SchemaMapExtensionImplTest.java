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

package org.apache.kylin.query.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.QueryExtension;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SchemaMapExtensionImplTest extends NLocalFileMetadataTestCase {
    private final String projectDefault = "default";
    private final String user1 = "u1";
    private final String group1 = "g1";
    private final String group2 = "g2";

    @Before
    public void setUp() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.extension.query.factory", "org.apache.kylin.query.QueryExtensionFactoryEnterprise");
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.extension.query.factory", "org.apache.kylin.query.QueryExtension$Factory");
        cleanupTestMetadata();
    }

    @Test
    public void testGetAuthorizedTablesAndColumns() {
        Set<String> groups = new HashSet<>();
        SchemaMapExtension extension = QueryExtension.getFactory().getSchemaMapExtension();
        groups.add(group1);
        groups.add(group2);
        Map<String, List<TableDesc>> result = extension.getAuthorizedTablesAndColumns(getTestConfig(),
                projectDefault, false, user1, groups);
        Assert.assertEquals(0, result.values().size());

        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        manager.updateAclTCR(new AclTCR(), group1, false);
        result = extension.getAuthorizedTablesAndColumns(getTestConfig(),
                projectDefault, false, user1, groups);

        Iterator<List<TableDesc>> iterator = result.values().iterator();
        List<TableDesc> tables = new ArrayList<>();
        while (iterator.hasNext()) {
            tables = iterator.next();
        }
        Assert.assertTrue(tables.size() > 0);
        Assert.assertTrue(tables.stream().anyMatch(tableDesc -> tableDesc.getName().equals("TEST_ACCOUNT")));
        Assert.assertTrue(tables.stream().anyMatch(tableDesc -> Arrays.stream(
                tableDesc.getColumns()).anyMatch(column -> column.getName().equals("LEAF_CATEG_ID"))));
    }
}
