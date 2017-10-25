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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.CubeService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.kyligence.kap.rest.controller2.CubeControllerV2;
import io.kyligence.kap.rest.response.KapCubeResponse;
import io.kyligence.kap.rest.service.ServiceTestBase;

/**
 */
public class CubeControllerTest extends ServiceTestBase {

    @Autowired
    private CubeControllerV2 cubeControllerV2;

    @Autowired
    CubeService cubeService;

    @Test
    public void testCubesOrder() throws IOException, InterruptedException {

        EnvelopeResponse firstListResponse = cubeControllerV2.getCubesPaging(null, false, null, "default", 0, 10);

        Assert.assertNotNull(firstListResponse.data);
        Assert.assertTrue(firstListResponse.data instanceof HashMap);
        Assert.assertTrue(((HashMap) firstListResponse.data).get("cubes") instanceof List);

        List<String> firstOrder = new ArrayList<>();
        List<String> secondOrder = new ArrayList<>();

        for (Object object : (List) ((HashMap) firstListResponse.data).get("cubes")) {
            Assert.assertTrue(object instanceof KapCubeResponse);
            KapCubeResponse cubeResponse = (KapCubeResponse) object;
            firstOrder.add(cubeResponse.getName());
            CubeDesc cubeDesc = cubeService.getCubeDescManager().getCubeDesc(cubeResponse.getDescName());
            cubeService.updateCubeAndDesc(cubeService.getCubeManager().getCube(cubeResponse.getName()), cubeDesc,
                    "default", true);
            Thread.sleep(1000);
        }

        EnvelopeResponse secondListResponse = cubeControllerV2.getCubesPaging(null, false, null, "default", 0, 10);
        for (Object object : (List) ((HashMap) secondListResponse.data).get("cubes")) {
            KapCubeResponse cubeResponse = (KapCubeResponse) object;
            secondOrder.add(cubeResponse.getName());
        }

        Assert.assertEquals(firstOrder.size(), secondOrder.size());

        for (int i = 0; i < firstOrder.size(); i++) {
            Assert.assertEquals(firstOrder.get(i), secondOrder.get(firstOrder.size() - 1 - i));
        }
    }
}
