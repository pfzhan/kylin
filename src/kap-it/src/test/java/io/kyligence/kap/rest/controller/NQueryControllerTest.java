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
import static org.hamcrest.core.StringContains.containsString;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.metadata.user.ManagedUser;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;
import lombok.val;

public class NQueryControllerTest extends AbstractMVCIntegrationTestCase {

    @Autowired
    protected UserService userService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        userService.createUser(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(new UserGrantedAuthority("ROLE_ADMIN"))));
    }

    @Test
    public void testQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("DEFAULT");
        sqlRequest.setSql("SELECT * FROM TEST_KYLIN_FACT");
        overwriteSystemProp("kylin.query.pushdown-enabled", "false");

        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(sqlRequest))
                        .header("User-Agent", "Chrome/89.0.4389.82 Safari/537.36")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        final boolean isException = JsonPath.compile("$.data.isException")
                .read(result.getResponse().getContentAsString());
        Assert.assertTrue(isException);

        final String exceptionMsg = JsonPath.compile("$.data.exceptionMessage")
                .read(result.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(exceptionMsg, "No realization found for OLAPContext"));
    }

    @Test
    public void testPushDownQuery() throws Exception {
        Class.forName("org.h2.Driver");
        overwriteSystemProp("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        overwriteSystemProp("kylin.query.pushdown-enabled", "true");
        overwriteSystemProp("kylin.query.pushdown.cache-enabled", "true");
        overwriteSystemProp("kylin.query.cache-threshold-duration", "0");

        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        overwriteSystemProp("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        overwriteSystemProp("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        overwriteSystemProp("kylin.query.pushdown.jdbc.username", "sa");
        overwriteSystemProp("kylin.query.pushdown.jdbc.password", "");

        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("Default");

        String queryFileName = "src/test/resources/query/sql_pushdown/query04.sql";
        File sqlFile = new File(queryFileName);
        String sql = KylinTestBase.getTextFromFile(sqlFile);
        sqlRequest.setSql(sql);

        // get h2 query result for comparison
        Statement statement = h2Connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .header("User-Agent", "Chrome/89.0.4389.82 Safari/537.36")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.results[0].length()")
                        .value(resultSet.getMetaData().getColumnCount()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.pushDown").value(true))
                .andDo(MockMvcResultHandlers.print()).andReturn();

        // push down can not hit cache because the cache is expired by default
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .header("User-Agent", "Chrome/89.0.4389.82 Safari/537.36")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.results[0].length()")
                        .value(resultSet.getMetaData().getColumnCount()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.pushDown").value(true))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.engineType").value(QueryContext.PUSHDOWN_RDBMS))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.storageCacheUsed").value(false))
                .andDo(MockMvcResultHandlers.print()).andReturn();

        h2Connection.close();
    }

    @Test
    public void testPrepareQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("Default");
        sqlRequest.setSql("SELECT * FROM test_country");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print());

    }

    @Test
    public void testGetMetadata() throws Exception {
        final val mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/query/tables_and_columns").param("project", "DEFAULT")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        final JsonNode data = jsonNode.get("data");
        Assert.assertFalse(data.toString().contains(ComputedColumnDesc.getComputedColumnInternalNamePrefix()));
    }

    @Test
    public void testGetQueryHistoryTableNames() throws Exception {
        List<String> projects = Arrays.asList("DEfault", "SSb");
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/query/history_queries/table_names")
                        .param("projects", StringUtils.join(projects, ","))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Map data = JsonUtil.readValue(jsonNode.get("data").toString(), Map.class);
        Set<String> actualProjects = data.keySet();
        Assert.assertEquals(2, actualProjects.size());
        Assert.assertTrue(actualProjects.contains("default"));
        Assert.assertTrue(actualProjects.contains("ssb"));

        mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/query/history_queries/table_names")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        data = JsonUtil.readValue(jsonNode.get("data").toString(), Map.class);
        actualProjects = data.keySet();

        int allProjectsSize = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects().size();
        Assert.assertEquals(allProjectsSize, actualProjects.size());
    }


    @Test
    public void testDownloadQueryResultWithQueryException() throws Exception {
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query/format/{format}", "csv")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED_VALUE).param("project", "Default")
                        .param("sql", "SELECT error").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals(1, content.length());
        Assert.assertEquals('\uFEFF', content.charAt(0));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/format/{format}", "csv")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED_VALUE).param("project", "Default")
                .param("sql", "SELECT 1").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andExpect(content().string(containsString("1")));
    }
}
