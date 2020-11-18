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

package io.kyligence.kap.tool.bisync.tableau;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.bisync.SyncModelBuilder;
import io.kyligence.kap.tool.bisync.SyncModelTestUtil;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class TableauDatasourceTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testTableauDataSource() throws IOException {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        val syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel();

        TableauDatasourceModel datasource = new TableauDataSourceConverter().convert(syncModel, syncContext);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        datasource.dump(outStream);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.tds"), outStream.toString());
    }

    @Test
    public void testTableauDataConnectorSource() throws IOException {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        val syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel();

        TableauDatasourceModel datasource = new TableauDataSourceConverter().convert(syncModel, syncContext);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        datasource.dump(outStream);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector.tds"), outStream.toString());
    }

    private String getExpectedTds(String path) throws IOException {
        return CharStreams.toString(new InputStreamReader(
                getClass().getResourceAsStream(path), Charsets.UTF_8));
    }
}
