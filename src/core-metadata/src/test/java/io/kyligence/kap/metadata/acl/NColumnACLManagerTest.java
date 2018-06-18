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

package io.kyligence.kap.metadata.acl;

import com.google.common.collect.Sets;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class NColumnACLManagerTest extends NLocalFileMetadataTestCase {

    private final String PROJECT = "default";
    private final String USER_NAME = "USER";
    private final String GROUP_NAME = "GROUP";
    private final String TABLE = "TEST_ORDER";

    private final String TEST_COLUMN_0 = "ORDER_ID";
    private final String TEST_COLUMN_1 = "BUYER_ID";
    private final String TEST_COLUMN_2 = "TEST_DATE_ENC";
    private final String TEST_COLUMN_3 = "TEST_TIME_ENC";
    private final Set<String> TEST_COLUMNS = Sets.newHashSet(TEST_COLUMN_1, TEST_COLUMN_2, TEST_COLUMN_3);

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testAddColumnACL() throws IOException {
        final ColumnACLManager manager = new ColumnACLManager(getTestConfig());

        manager.addColumnACL(PROJECT, USER_NAME, TABLE, TEST_COLUMNS, MetadataConstants.TYPE_USER);
        manager.addColumnACL(PROJECT, GROUP_NAME, TABLE, TEST_COLUMNS, MetadataConstants.TYPE_GROUP);

        final ColumnACL actual = getStore().getResource("/column_acl/" + PROJECT, ColumnACL.class, new JsonSerializer<>(ColumnACL.class));
        Assert.assertTrue(actual.contains(USER_NAME, MetadataConstants.TYPE_USER));
        Assert.assertTrue(actual.contains(GROUP_NAME, MetadataConstants.TYPE_GROUP));
    }

    @Test
    public void testGetColumnACLByCache() throws IOException {
        final ColumnACLManager manager = new ColumnACLManager(getTestConfig());

        manager.addColumnACL(PROJECT, USER_NAME, TABLE, TEST_COLUMNS, MetadataConstants.TYPE_USER);

        final ColumnACL actual = manager.getColumnACLByCache(PROJECT);
        Assert.assertTrue(actual.contains(USER_NAME, MetadataConstants.TYPE_USER));
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }
}
