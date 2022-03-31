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

package io.kyligence.kap.query.engine;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.query.QueryExtension;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaMapExtensionImplTest extends NLocalFileMetadataTestCase {
    private final String projectDefault = "default";
    private final String user1 = "u1";
    private final String group1 = "g1";
    private final String group2 = "g2";

    @Before
    public void setUp() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
    }

    @After
    public void tearDown() {
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
