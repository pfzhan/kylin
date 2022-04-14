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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.controller.v2.NUserControllerV2;

public class NUserControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private NUserController nUserController;

    @Mock
    Environment env;

    @InjectMocks
    private NUserControllerV2 nUserControllerV2 = Mockito.spy(new NUserControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setupResource() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.env", "UT");
        nUserController = Mockito.spy(new NUserController());
    }

    @Before
    public void setup() {
        FilterProvider filterProvider = new SimpleFilterProvider().addFilter("passwordFilter",
                SimpleBeanPropertyFilter.serializeAllExcept("password", "defaultPassword"));
        ObjectMapper objectMapper = new ObjectMapper().setFilterProvider(filterProvider);
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(objectMapper);

        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(true).when(env).acceptsProfiles("testing", "custom");
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(nUserControllerV2).setMessageConverters(converter)
                .setContentNegotiationManager(contentNegotiationManager).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        ManagedUser user = new ManagedUser("ADMIN", "ADMIN", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testListAllUsers() throws Exception {
        ManagedUser user1 = new ManagedUser();
        user1.setUsername("ADMIN");
        user1.setPassword("KYLIN");
        user1.setDefaultPassword(false);
        List<ManagedUser> managedUsers = Lists.newArrayList(user1);
        Mockito.when(nUserController.listAllUsers("KYLIN", false, 0, 10)).thenReturn(new EnvelopeResponse<>(
                KylinException.CODE_SUCCESS, DataResult.get(managedUsers, 0, 10), "testListAllUsers"));

        mockMvc.perform(MockMvcRequestBuilders.get("/api/kap/user/users").contentType(MediaType.APPLICATION_JSON)
                .param("name", "KYLIN").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nUserControllerV2).listAllUsers("KYLIN", false, 0, 10);
    }

    @Test
    public void testBasics() {
        EnvelopeResponse<UserDetails> userDetailsEnvelopeResponse = nUserControllerV2.authenticatedUser();
        Assert.assertNotNull(userDetailsEnvelopeResponse);
        Assert.assertTrue(userDetailsEnvelopeResponse.getCode().equals(KylinException.CODE_SUCCESS));
    }
}
