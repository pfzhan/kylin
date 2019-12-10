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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.hamcrest.CoreMatchers.containsString;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.service.AclTCRService;

public class NAccessControllerTest {

    private MockMvc mockMvc;

    @Mock
    private AccessService accessService;

    @Mock
    private UserService userService;

    @Mock
    private AclTCRService aclTCRService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NAccessController nAccessController = Mockito.spy(new NAccessController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nAccessController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetUserPermissionInPrj() throws Exception {
        List<JobStatusEnum> status = new ArrayList<>();
        status.add(JobStatusEnum.NEW);
        ArrayList<AbstractExecutable> jobs = new ArrayList<>();
        Integer[] statusInt = { 4 };
        String[] subjects = {};
        Mockito.when(accessService.getUserPermissionInPrj("default")).thenReturn("ADMIN");
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/access/permission/project_permission")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nAccessController).getUserPermissionInPrj("default");
    }

    @Test
    public void testGrantPermissionForValidUser() throws Exception {
        String type = "ProjectInstance";
        String uuid = "u126snk32242152";
        String sid = "user_g1";
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        Mockito.doNothing().when(accessService).grant(type, uuid, "1", true, "ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).grant(type, uuid, accessRequest);
    }

    @Test
    public void testGrantPermissionForEmptyUser() throws Exception {
        String sid = "";
        String expectedErrorMsg = "User/Group name should not be empty.";
        testGrantPermissionForUser(sid, expectedErrorMsg);
    }

    @Test
    @Ignore
    public void testGrantPermissionForInvalidUser() throws Exception {
        String sid = "1/";
        String expectedErrorMsg = "User/Group name should only contain alphanumerics and underscores.";
        testGrantPermissionForUser(sid, expectedErrorMsg);
    }

    private void testGrantPermissionForUser(String sid, String expectedMsg) throws Exception {
        String type = "ProjectInstance";
        String uuid = "u428vfn31748";
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        Mockito.doNothing().when(accessService).grant(type, uuid, "1", true, "ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString(expectedMsg)));
        Mockito.verify(nAccessController).grant(type, uuid, accessRequest);
    }

}
