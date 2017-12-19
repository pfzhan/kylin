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

package io.kyligence.kap.smart;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.smart.query.QueryRecord;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;

public class NSmartQueryMockupTest extends NLocalFileMetadataTestCase {
    private static final String[] SQL_EXTS = { "sql" };
    private static final String PROJ_NAME = "default";

    private static final String KAP_QUERY_DIR = "resources/query/";
    private static final String KYLIN_QUERY_DIR = "../../kylin/kylin-it/src/test/resources/query/";

    private MockupQueryExecutor exec = new MockupQueryExecutor();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCommon() throws IOException {
        testRoundTrip(KYLIN_QUERY_DIR + "sql");
    }

    private void testRoundTrip(String sqlDirectory) throws IOException {
        String[] sqls = readSQLs(sqlDirectory);

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), PROJ_NAME, sqls);
        smartMaster.runAll();

        for (String sql : sqls)
            verifySQLs(sql);
    }

    private String[] readSQLs(String sqlDirectory) throws IOException {
        File sqlDir = new File(sqlDirectory);
        Preconditions.checkArgument(sqlDir.exists(), "SQL Directory not found.");
        Collection<File> sqlFiles = FileUtils.listFiles(sqlDir, SQL_EXTS, true);
        if (CollectionUtils.isEmpty(sqlFiles))
            return new String[0];

        String[] sqls = new String[sqlFiles.size()];
        int i = 0;
        for (File sqlFile : sqlFiles) {
            sqls[i++] = FileUtils.readFileToString(sqlFile);
        }
        return sqls;
    }

    private void verifySQLs(String sql) {
        QueryRecord qr = exec.execute(PROJ_NAME, sql);
        Collection<OLAPContext> ctxs = qr.getOLAPContexts();
        // more assertion here
        System.out.println(qr);
    }
}
