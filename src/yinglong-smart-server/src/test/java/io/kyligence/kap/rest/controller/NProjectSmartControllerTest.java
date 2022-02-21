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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.service.ProjectSmartService;

public class NProjectSmartControllerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private MockMvc mockMvc;

    @Mock
    private ProjectSmartService projectSmartService;

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private NProjectSmartController nProjectSmartController = Mockito.spy(new NProjectSmartController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nProjectSmartController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateFrequencyRule() throws Exception {
        String project = "default";
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(project);
        request.setFreqEnable(false);
        request.setMinHitCount("1");
        request.setUpdateFrequency("1");
        request.setEffectiveDays("1");
        request.setRecommendationsValue("1");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nProjectSmartController).updateFavoriteRules(Mockito.any(request.getClass()));
    }

    @Test
    public void testCheckUpdateFavoriteRuleArgsWithEmtpyFrequency() {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setMinHitCount("1");
        request.setEffectiveDays("1");
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getUPDATE_FREQUENCY_NOT_EMPTY());
        NProjectSmartController.checkUpdateFavoriteRuleArgs(request);
    }

    @Test
    public void testCheckUpdateFavoriteRuleArgsWithEmptyHitCount() {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getMIN_HIT_COUNT_NOT_EMPTY());
        NProjectSmartController.checkUpdateFavoriteRuleArgs(request);
    }

    @Test
    public void testCheckUpdateFavoriteRuleArgsWithEmpty() {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setMinHitCount("1");
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getEFFECTIVE_DAYS_NOT_EMPTY());
        NProjectSmartController.checkUpdateFavoriteRuleArgs(request);
    }

    @Test
    public void testUpdateFrequencyRuleWithWrongArgs() throws Exception {
        String project = "default";
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(project);
        request.setFreqEnable(true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        request.setFreqEnable(false);
        request.setDurationEnable(true);
        request.setMinDuration("0");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
    }

    @Test
    public void testGetFrequencyRule() throws Exception {
        String project = "default";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).param("project", project)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nProjectSmartController).getFavoriteRules(project);
    }
}
