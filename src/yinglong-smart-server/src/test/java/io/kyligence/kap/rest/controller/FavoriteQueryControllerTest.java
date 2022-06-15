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

import java.io.File;
import java.io.FileInputStream;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.SQLValidateRequest;
import io.kyligence.kap.rest.service.FavoriteRuleService;

public class FavoriteQueryControllerTest extends NLocalFileMetadataTestCase {

    private final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private FavoriteRuleService favoriteRuleService;
    @InjectMocks
    private final FavoriteQueryController favoriteQueryController = Mockito.spy(new FavoriteQueryController());

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(favoriteQueryController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testImportSqls() throws Exception {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        MockMultipartFile file = new MockMultipartFile("files", "sqls.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql")));
        MockMultipartFile file2 = new MockMultipartFile("files", "sqls.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt")));
        mockMvc.perform(MockMvcRequestBuilders.multipart("/api/query/favorite_queries/sql_files").file(file).file(file2)
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).importSqls(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testSqlValidate() throws Exception {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        SQLValidateRequest request = new SQLValidateRequest(PROJECT, "sql");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/sql_validation")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).sqlValidate(Mockito.any(SQLValidateRequest.class));
    }
}
