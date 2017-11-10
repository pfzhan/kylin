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

package io.kyligence.kap.rest.controller;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.ModelRequest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.rest.controller2.ModelControllerV2;
import io.kyligence.kap.rest.service.KapModelService;
import io.kyligence.kap.rest.service.ServiceTestBase;

public class ModelControllerTest extends ServiceTestBase {

    @Autowired
    private ModelControllerV2 modelControllerV2;

    @Autowired
    private KapModelService kapModelService;

    @Test
    public void testUpdateModelDescV2() throws IOException {
        String cubeName = "test_streaming_table_cube";
        String[] mps = { "SITE" };

        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
        KapModel kapModel = (KapModel) cubeInstance.getDescriptor().getModel();
        kapModel.setOwner("A");

        String modelDesc = JsonUtil.writeValueAsString(kapModel);
        ModelRequest modelRequest = new ModelRequest();
        modelRequest.setModelDescData(modelDesc);
        modelRequest.setProject("default");

        modelControllerV2.updateModelDescV2(modelRequest);

        kapModel.setMutiLevelPartitionColStrs(mps);
        modelDesc = JsonUtil.writeValueAsString(kapModel);
        modelRequest.setModelDescData(modelDesc);

        try {
            modelControllerV2.updateModelDescV2(modelRequest);
            fail("No exception thrown.");
        } catch (Exception ex) {
            assertTrue(ex instanceof BadRequestException);
            assertTrue(ex.getMessage().contains("Sorry, multi-level partitioned model cannot work"));
        }

    }
}
