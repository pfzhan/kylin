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

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;

public class DeleteFavoriteQueryCLITest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void test() {
        KylinConfig systemKylinConfig = KylinConfig.getInstanceFromEnv();
        systemKylinConfig.setProperty("kylin.env", "PROD");

        FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(systemKylinConfig, "broken_test");
        Assert.assertTrue(favoriteRuleManager.getAll().size() > 0);

        DeleteFavoriteQueryCLI deleteFavoriteQueryCLI = new DeleteFavoriteQueryCLI();
        deleteFavoriteQueryCLI.execute(new String[] { "-d", getTestConfig().getMetadataUrl().toString(), "-e" });

        Assert.assertEquals(favoriteRuleManager.getAll().size(), 0);
    }
}