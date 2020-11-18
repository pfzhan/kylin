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

package io.kyligence.kap.tool.bisync;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SyncModelBuilderTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBuildSyncModel() {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        val syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel();
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        val model = df.getModel();

        Assert.assertEquals(project, syncModel.getProjectName());
        Assert.assertEquals(model.getAlias(), syncModel.getModelName());
        Assert.assertEquals("localhost", syncModel.getHost());
        Assert.assertEquals("7070", syncModel.getPort());
        val factTable = syncModel.getJoinTree().getValue();
        Assert.assertEquals(1, syncModel.getJoinTree().getChildNodes().size());

        val lookupTable = syncModel.getJoinTree().getChildNodes().get(0).getValue();
        Assert.assertEquals("TEST_MEASURE", factTable.getAlias());
        Assert.assertEquals(NDataModel.TableKind.FACT, factTable.getKind());
        Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, factTable.getJoinRelationTypeEnum());
        Assert.assertEquals("TEST_MEASURE1", lookupTable.getAlias());
        Assert.assertEquals(NDataModel.TableKind.LOOKUP, lookupTable.getKind());
        Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, lookupTable.getJoinRelationTypeEnum());

        val modelCol = model.getEffectiveCols().values().iterator().next();
        Assert.assertEquals(model.getEffectiveCols().size(), syncModel.getColumnDefMap().size());
        val syncCol = syncModel.getColumnDefMap().get(modelCol.getIdentity());
        Assert.assertEquals(modelCol.getTableAlias(), syncCol.getTableAlias());
        Assert.assertEquals(modelCol.getName(), syncCol.getColumnName());
        Assert.assertEquals(modelCol.getColumnDesc().getName(), syncCol.getColumnAlias());
        Assert.assertEquals("dimension", syncCol.getRole());

        Assert.assertEquals(model.getAllMeasures().size(), syncModel.getMetrics().size());
        val syncMeasure = syncModel.getMetrics().get(0).getMeasure();
        val modelMeasure = model.getAllMeasures().stream().filter(m -> m.getId() == syncMeasure.getId()).findFirst().get();
        Assert.assertEquals(modelMeasure, syncMeasure);
    }
}
