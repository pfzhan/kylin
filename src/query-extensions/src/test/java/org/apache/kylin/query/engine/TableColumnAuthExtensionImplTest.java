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

import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.query.QueryExtension;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
@OverwriteProp(key = "kylin.query.security.acl-tcr-enabled", value = "true")
class TableColumnAuthExtensionImplTest {
    private final String projectDefault = "default";
    private final String user1 = "u1";
    private final String group1 = "g1";
    private final String group2 = "g2";

    @BeforeEach
    private void setUp() {
        getTestConfig().setProperty("kylin.extension.query.factory", "org.apache.kylin.query.QueryExtensionFactoryEnterprise");
    }

    @AfterEach
    private void destroy() {
        getTestConfig().setProperty("kylin.extension.query.factory", "org.apache.kylin.query.QueryExtension$Factory");
    }

    @Test
    void testIsColumnsAuthorized() {
        TableColumnAuthExtension extension = QueryExtension.getFactory().getTableColumnAuthExtension();

        Set<String> groups = new HashSet<>();
        groups.add(group1);
        groups.add(group2);

        Set<String> columns = new HashSet<>();
        columns.add("DEFAULT.TEST_COUNTRY.COUNTRY");
        Assert.assertFalse(extension.isColumnsAuthorized(getTestConfig(),
                projectDefault, user1, groups, columns));

        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        columns.add("DEFAULT.TEST_KYLIN_FACT.TRANS_ID");
        Assert.assertTrue(extension.isColumnsAuthorized(getTestConfig(),
                projectDefault, user1, groups, columns));
    }

    KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }
}
