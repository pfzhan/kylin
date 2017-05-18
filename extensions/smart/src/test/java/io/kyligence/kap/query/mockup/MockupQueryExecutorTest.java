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
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class MockupQueryExecutorTest extends LocalFileMetadataTestCase {
    @After
    public void cleanup() throws IOException {
        cleanAfterClass();
    }

    @Before
    public void setup() throws IOException {
        createTestMetadata();

        KylinConfig mockupConfig = KylinConfig.getInstanceFromEnv();
        mockupConfig = Utils.newKylinConfig(mockupConfig.getMetadataUrl().toString());
        KylinConfig.setKylinConfigThreadLocal(mockupConfig);
    }

    @Test
    public void test() throws SQLException, IOException {
        TestQueryRecorder queryRecorder = new TestQueryRecorder();
        MockupQueryExecutor executor = new MockupQueryExecutor(queryRecorder);
        executor.execute("default", "select count(*) from STREAMING_TABLE where DAY_START is not null");
        executor.close();

        Object[] result = queryRecorder.getResult();
        validateResult((CubeInstance) result[0], (GTCubeStorageQueryRequest) result[1]);
    }

    private void validateResult(CubeInstance cubeInstance, GTCubeStorageQueryRequest gtRequest) {
        Assert.assertEquals("test_streaming_table_cube", cubeInstance.getName());
        Assert.assertEquals(4L, gtRequest.getCuboid().getId());
        Assert.assertEquals(1, gtRequest.getMetrics().size());
        Assert.assertEquals(1, gtRequest.getFilterCols().size());
        Assert.assertEquals(1, gtRequest.getDimensions().size());
        Assert.assertTrue(gtRequest.getFilter() instanceof CompareTupleFilter);
    }

    private class TestQueryRecorder extends AbstractQueryRecorder<Object[]> {
        CubeInstance cubeInstance;
        GTCubeStorageQueryRequest gtRequest;

        @Override
        public void record(CubeInstance cubeInstance, GTCubeStorageQueryRequest gtRequest) {
            this.cubeInstance = cubeInstance;
            this.gtRequest = gtRequest;

            validateResult(cubeInstance, gtRequest);
        }

        public Object[] getResult() {
            return new Object[] { cubeInstance, gtRequest };
        }
    }
}
