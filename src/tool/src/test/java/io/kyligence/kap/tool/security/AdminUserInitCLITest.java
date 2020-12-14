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

package io.kyligence.kap.tool.security;

import io.kyligence.kap.tool.garbage.StorageCleaner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import lombok.val;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class AdminUserInitCLITest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testInitAdminUser() throws Exception {
        // before create admin user
        val config = KylinConfig.getInstanceFromEnv();
        NKylinUserManager beforeCreateAdminManager = NKylinUserManager.getInstance(config);
        Assert.assertEquals(0, beforeCreateAdminManager.list().size());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output));

        // metadata without user, create admin user
        AdminUserInitCLI.initAdminUser(true);
        // clear cache, reload metadata
        ResourceStore.clearCache(config);
        config.clearManagers();
        NKylinUserManager afterCreateAdminManager = NKylinUserManager.getInstance(config);
        Assert.assertTrue(afterCreateAdminManager.exists("ADMIN"));

        // assert output on console
        Assert.assertTrue(output.toString().startsWith(StorageCleaner.ANSI_RED + "Create default user finished. The username of initialized user is [" +
                StorageCleaner.ANSI_RESET + "ADMIN" + StorageCleaner.ANSI_RED + "], which password is "));
        Assert.assertTrue(output.toString().endsWith("Please keep the password properly. " +
                "And if you forget the password, you can reset it according to user manual." + StorageCleaner.ANSI_RESET + "\n"));

        System.setOut(System.out);

        // already have admin user
        AdminUserInitCLI.initAdminUser(true);
        // clear cache, reload metadata
        ResourceStore.clearCache(config);
        config.clearManagers();
        NKylinUserManager afterCreateAdminManager2 = NKylinUserManager.getInstance(config);
        Assert.assertEquals(1, afterCreateAdminManager2.list().size());
    }

    @Test
    public void testGenerateRandomPassword() {
        String password = AdminUserInitCLI.generateRandomPassword();
        Assert.assertTrue(AdminUserInitCLI.PASSWORD_PATTERN.matcher(password).matches());
    }
}
