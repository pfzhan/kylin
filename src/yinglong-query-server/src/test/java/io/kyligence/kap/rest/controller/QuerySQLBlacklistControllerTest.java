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
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.service.QuerySQLBlacklistService;

import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.blacklist.SQLBlacklist;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

public class QuerySQLBlacklistControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;
    private static final String PROJECT = "default";

    @Mock
    private QuerySQLBlacklistService querySQLBlacklistService;

    @InjectMocks
    private QuerySQLBlacklistController querySQLBlacklistController = Mockito.spy(new QuerySQLBlacklistController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(querySQLBlacklistController)
                .defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        super.createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        when(querySQLBlacklistService.getSqlBlacklist(PROJECT)).thenReturn(sqlBlacklist);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/query_sql_blacklist/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(querySQLBlacklistController).getSqlBlacklist(PROJECT);
    }

    @Test
    public void testAddItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        SQLBlacklistItemRequest request = new SQLBlacklistItemRequest();
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/add_item/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        ErrorCode errorCode = ((KylinException)mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028003", errorCode.getCodeString());

        request.setSql(sql);
        request.setRegex(".*");
        request.setConcurrentLimit(0);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/add_item/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    @Test
    public void testOverwrite() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        SQLBlacklistItemRequest sqlBlacklistItemRequest = new SQLBlacklistItemRequest();
        sqlBlacklistItemRequest.setSql(sql);
        sqlBlacklistItemRequest.setRegex(".*");
        sqlBlacklistItemRequest.setConcurrentLimit(0);

        SQLBlacklistRequest request = new SQLBlacklistRequest();
        request.setBlacklistItems(Lists.newArrayList(sqlBlacklistItemRequest));


        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/overwrite")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        ErrorCode errorCode = ((KylinException)mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028004", errorCode.getCodeString());

        request.setProject(PROJECT);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/overwrite")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    @Test
    public void testUpdateItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        SQLBlacklistItemRequest sqlBlacklistItemRequest = new SQLBlacklistItemRequest();

        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/update_item/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(sqlBlacklistItemRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        ErrorCode errorCode = ((KylinException)mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028007", errorCode.getCodeString());

        sqlBlacklistItemRequest.setId("5b7bcee9-e22f-beb6-b14d-4f8ce01b1446");
        mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/update_item/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(sqlBlacklistItemRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        errorCode = ((KylinException)mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028003", errorCode.getCodeString());

        sqlBlacklistItemRequest.setSql(sql);
        sqlBlacklistItemRequest.setRegex(".*");
        sqlBlacklistItemRequest.setConcurrentLimit(0);
    }

    @Test
    public void testDeleteItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);

        when(querySQLBlacklistService.deleteSqlBlacklistItem(PROJECT, "0")).thenReturn(sqlBlacklist);

        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.delete("/api/query_sql_blacklist/delete_item/{project}/{id}", PROJECT, "0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(querySQLBlacklistController).deleteItem(PROJECT, "0");
    }
}
