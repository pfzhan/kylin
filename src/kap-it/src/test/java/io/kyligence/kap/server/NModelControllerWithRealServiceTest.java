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
package io.kyligence.kap.server;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.ComputedColumnCheckRequest;
import io.kyligence.kap.rest.request.ModelRequest;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

public class NModelControllerWithRealServiceTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testCheckComputedColumns() throws Exception {

        final ComputedColumnCheckRequest ccRequest = new ComputedColumnCheckRequest();
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        model.getComputedColumnDescs().get(0).setColumnName("rename_cc");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        ccRequest.setModelDesc(modelRequest);
        ccRequest.setProject("default");

        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/computed_columns/check")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(ccRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }
}
