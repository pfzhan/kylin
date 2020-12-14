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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.rest.request.DDLRequest;
import io.kyligence.kap.rest.request.ExportTableRequest;
import io.kyligence.kap.rest.service.SparkSourceService;

public class SparkSourceControllerTest {
    private MockMvc mockMvc;

    @Mock
    private SparkSourceService sparkSourceService;

    @InjectMocks
    private SparkSourceController sparkSourceController = Mockito.spy(new SparkSourceController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(sparkSourceController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testExecuteSQL() throws Exception {
        DDLRequest ddlRequest = new DDLRequest();
        ddlRequest.setSql("show databases");
        ddlRequest.setDatabase("default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/spark_source/execute").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(ddlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).executeSQL(ddlRequest);
    }

    @Test
    public void testExportTableStructuree() throws Exception {
        ExportTableRequest request = new ExportTableRequest();
        request.setDatabases("SSB");
        request.setTables(new String[] { "LINEORDER", "DATES" });
        mockMvc.perform(MockMvcRequestBuilders.post("/api/spark_source/export_table_structure")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(sparkSourceController).exportTableStructure(request);
    }

    @Test
    public void testDropTable() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/spark_source/{database}/tables/{table}", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).dropTable("default", "COUNTRY");
    }

    @Test
    public void testListDatabase() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/databases")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).listDatabase();
    }

    @Test
    public void testListTables() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/tables", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).listTables("default", "test");
    }

    @Test
    public void testListColumns() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/columns", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).listColumns("default", "COUNTRY");
    }

    @Test
    public void testGetTableDesc() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/desc", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).getTableDesc("default", "COUNTRY");
    }

    @Test
    public void testHasPartition() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/spark_source/{database}/{table}/has_partition", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).hasPartition("default", "COUNTRY");
    }

    @Test
    public void testDatabaseExists() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/exists", "default")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).databaseExists("default");
    }

    @Test
    public void testTableExists() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/exists", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).tableExists("default", "COUNTRY");
    }

    @Test
    public void testLoadSamples() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/load_samples")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).loadSamples();
    }

    @Test
    public void testMsck() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/msck", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).msck("default", "COUNTRY");
    }
}
