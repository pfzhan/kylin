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

package io.kyligence.kap.metadata.streaming;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FusionModelManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private FusionModelManager mgr;
    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = FusionModelManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetFusionModel() {
        val emptyId = "";
        Assert.assertNull(mgr.getFusionModel(emptyId));

        val id = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val fusionModel = mgr.getFusionModel(id);
        Assert.assertNotNull(fusionModel);

        val id_not_existed = "b05034a8-c037-416b-aa26-9e6b4a41ee41";
        val notExisted = mgr.getFusionModel(id_not_existed);
        Assert.assertNull(notExisted);
    }

    @Test
    public void testNewFusionModel() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val batchModel = modelManager.getDataModelDesc("334671fd-e383-4fc9-b5c2-94fce832f77a");
        val streamingModel = modelManager.getDataModelDesc("e78a89dd-847f-4574-8afa-8768b4228b74");
        FusionModel fusionModel = new FusionModel(streamingModel, batchModel);

        FusionModel copy = new FusionModel(fusionModel);

        mgr.createModel(copy);
        Assert.assertNotNull(mgr.getFusionModel("e78a89dd-847f-4574-8afa-8768b4228b74"));

        Assert.assertEquals(2, copy.getModelsId().size());

        Assert.assertEquals("e78a89dd-847f-4574-8afa-8768b4228b74", copy.resourceName());
        Assert.assertEquals("334671fd-e383-4fc9-b5c2-94fce832f77a", copy.getBatchModel().getUuid());
    }

    @Test
    public void testDropFusionModel() {
        val id = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val fusionModel = mgr.dropModel(id);
        Assert.assertNotNull(fusionModel);

        Assert.assertNull(mgr.getFusionModel(id));

        val id_not_existed = "b05034a8-c037-416b-aa26-9e6b4a41ee41";
        val notExisted = mgr.dropModel(id_not_existed);
        Assert.assertNull(notExisted);
    }

    @Test
    public void testFusionModelCheck() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val batchModel = modelManager.getDataModelDesc("334671fd-e383-4fc9-b5c2-94fce832f77a");
        val streamingModel = modelManager.getDataModelDesc("b05034a8-c037-416b-aa26-9e6b4a41ee40");

        Assert.assertTrue(batchModel.skipFusionModel());
        Assert.assertTrue(batchModel.isFusionModel());
        Assert.assertTrue(streamingModel.showFusionModel());
    }

}
