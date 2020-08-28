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
package io.kyligence.kap.tool.upgrade;

import static org.apache.kylin.common.persistence.ResourceStore.USER_GROUP_ROOT;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.usergroup.UserGroup;

public class UpdateUserGroupCLITest extends NLocalFileMetadataTestCase {
    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpgrade() throws Exception {
        String metadata = KylinConfig.getInstanceFromEnv().getMetadataUrl().toString();
        File path = new File(metadata);
        File userGroupFile = new File(path, USER_GROUP_ROOT);
        Assert.assertTrue(userGroupFile.isFile());
        UpdateUserGroupCLI tool = new UpdateUserGroupCLI();
        tool.execute(new String[] { "-metadata_dir", path.getAbsolutePath(), "-e" });
        Assert.assertTrue(userGroupFile.isDirectory());
        Assert.assertEquals(4, userGroupFile.listFiles().length);
        Set<String> groupNames = new HashSet<>();
        for (File file : userGroupFile.listFiles()) {
            UserGroup userGroup = JsonUtil.readValue(file, UserGroup.class);
            groupNames.add(userGroup.getGroupName());
            Assert.assertEquals(userGroup.getGroupName(), file.getName());
        }
        Assert.assertEquals(4, groupNames.size());
        Assert.assertTrue(groupNames.contains("ALL_USERS"));
        Assert.assertTrue(groupNames.contains("ROLE_ADMIN"));
        Assert.assertTrue(groupNames.contains("ROLE_ANALYST"));
        Assert.assertTrue(groupNames.contains("ROLE_MODELER"));
    }

}
