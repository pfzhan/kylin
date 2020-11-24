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

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.LogOutputTestCase;
import io.kyligence.kap.tool.util.MetadataUtil;

public class UpdateSessionTableCLITest extends LogOutputTestCase {

    private UpdateSessionTableCLI updateSessionTableCLI = Mockito.spy(new UpdateSessionTableCLI());

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSessionTableNotExist() throws Exception {
        KylinConfig config = getTestConfig();
        DataSource dataSource = MetadataUtil.getDataSource(config);
        ReflectionTestUtils.setField(updateSessionTableCLI, "dataSource", dataSource);
        updateSessionTableCLI.affectedRowsWhenTruncate("not exist");
        updateSessionTableCLI.truncateSessionTable("not exist");
        updateSessionTableCLI.isSessionTableNeedUpgrade("not exist");
        Assert.assertTrue(containsLog("Table not exist is not exist, affected rows is zero."));
        Assert.assertTrue(containsLog("Table not exist is not exist, skip truncate."));
        Assert.assertTrue(containsLog("Table not exist is not exist, no need to upgrade."));
    }

}
