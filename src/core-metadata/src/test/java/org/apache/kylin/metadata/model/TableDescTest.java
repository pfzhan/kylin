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

package org.apache.kylin.metadata.model;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.kyligence.kap.metadata.model.NTableMetadataManager.getInstance;

public class TableDescTest extends NLocalFileMetadataTestCase {
    private final String project = "default";
    private NTableMetadataManager tableMetadataManager;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        tableMetadataManager = getInstance(getTestConfig(), project);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testTransactional() {
        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        Assert.assertFalse(tableDesc.isTransactional());
        Assert.assertEquals("`DEFAULT`.`TEST_KYLIN_FACT_HIVE_TX_INTERMEDIATE`",
                tableDesc.getTransactionalTableIdentity(""));
        Assert.assertEquals("`DEFAULT`.`TEST_KYLIN_FACT_HIVE_TX_INTERMEDIATE_SUFFIX`",
                tableDesc.getTransactionalTableIdentity("_suffix"));
    }

    @Test
    public void testRangePartition() {
        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        Assert.assertFalse(tableDesc.isRangePartition());
    }
}
