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

package io.kyligence.kap.query.mockup;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.smart.query.AbstractQueryRecorder;
import io.kyligence.kap.smart.query.QueryRecord;
import io.kyligence.kap.smart.query.Utils;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;
import io.kyligence.kap.storage.NDataStorageQueryRequest;

public class MockupQueryExecutorTest extends NLocalFileMetadataTestCase {
    @After
    public void cleanup() throws IOException {
        this.cleanupTestMetadata();
        System.clearProperty("kap.query.engine.sparder-enabled");
    }

    @Before
    public void setup() throws IOException {
        this.createTestMetadata();

        System.setProperty("kap.query.engine.sparder-enabled", "false");

        KylinConfig mockupConfig = getTestConfig();
        mockupConfig = Utils.newKylinConfig(mockupConfig.getMetadataUrl().toString());
        Utils.exposeAllTableAndColumn(mockupConfig);
        mockupConfig.setProperty("kylin.metadata.realization-providers", "io.kyligence.kap.cube.model.NDataflowManager");
        KylinConfig.setKylinConfigThreadLocal(mockupConfig);
    }

    @Test
    public void test() throws SQLException, IOException {
        TestQueryRecorder queryRecorder = new TestQueryRecorder();
        MockupQueryExecutor executor = new MockupQueryExecutor();

        QueryRecord record = executor.execute("default",
                "select lstg_format_name, sum(price) as GMV from test_kylin_fact where lstg_format_name='FP-GTC' group by lstg_format_name ");
        queryRecorder.record(record);

        Object[] result = queryRecorder.getResult();
        validateResult((NDataflow) result[0], (NDataStorageQueryRequest) result[1]);
    }

    private void validateResult(NDataflow dataflow, NDataStorageQueryRequest gtRequest) {
        Assert.assertEquals("ncube_basic", dataflow.getName());
        Assert.assertEquals(1000001L, gtRequest.getCuboidLayout().getId());
        Assert.assertEquals(1, gtRequest.getMetrics().size());
        Assert.assertEquals(1, gtRequest.getFilterCols().size());
        Assert.assertEquals(1, gtRequest.getDimensions().size());
        Assert.assertTrue(gtRequest.getFilter() instanceof CompareTupleFilter);
    }

    private class TestQueryRecorder extends AbstractQueryRecorder<Object[]> {
        NDataflow dataflow;
        NDataStorageQueryRequest gtRequest;

        @Override
        public void record(QueryRecord queryRecord) {
            this.dataflow = queryRecord.getCubeInstance();
            this.gtRequest = queryRecord.getGtRequest();

            validateResult(dataflow, gtRequest);
        }

        public Object[] getResult() {
            return new Object[] { dataflow, gtRequest };
        }
    }
}
