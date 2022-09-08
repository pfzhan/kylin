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

package org.apache.kylin.query.metadata.query;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.MetadataExtension;
import org.apache.kylin.metadata.query.QueryExcludedTablesExtension;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class QueryExcludedTablesExtensionImplTest {
    private static String PROJECT = "default";
    private static String TABLE = "TEST_ACCOUNT";

    @BeforeEach
    private void setUp() {
        getTestConfig().setProperty("kylin.extension.metadata.factory", "org.apache.kylin.metadata.MetadataExtensionFactoryEnterprise");
    }

    @AfterEach
    private void destroy() {
        getTestConfig().setProperty("kylin.extension.metadata.factory", "org.apache.kylin.metadata.MetadataExtension$Factory");
    }

    @Test
    void testExcludedTables() {
        QueryExcludedTablesExtension extension = MetadataExtension.getFactory().getQueryExcludedTablesExtension();
        Assert.assertTrue(extension.getExcludedTables(getTestConfig(), PROJECT).isEmpty());

        extension.addExcludedTables(getTestConfig(), PROJECT, TABLE, false);
        Assert.assertTrue(extension.getExcludedTables(getTestConfig(), PROJECT).isEmpty());

        extension.addExcludedTables(getTestConfig(), PROJECT, TABLE, true);
        Assert.assertEquals(1, extension.getExcludedTables(getTestConfig(), PROJECT).size());
    }

    KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }
}
