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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import lombok.val;

public class NQueryControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("default");
        sqlRequest.setSql("-- This is comment" + '\n' + "SELECT * FROM TEST_KYLIN_FACT");
        sqlRequest.setUser_defined_tag("user_tag");
        System.setProperty("kylin.query.pushdown-enabled", "false");

        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(sqlRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        final boolean isException = JsonPath.compile("$.data.isException")
                .read(result.getResponse().getContentAsString());
        Assert.assertTrue(isException);

        final String exceptionMsg = JsonPath.compile("$.data.exceptionMessage")
                .read(result.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(exceptionMsg, "No realization found for OLAPContext"));
        System.clearProperty("kylin.query.pushdown-enabled");
    }

    @Test
    public void testUserTagExceedLimitation() throws Exception {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("default");
        sqlRequest.setSql("-- This is comment" + '\n' + "SELECT * FROM TEST_KYLIN_FACT");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i <= 256; i++) {
            builder.append('a');
        }
        sqlRequest.setUser_defined_tag(builder.toString());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andExpect(jsonPath("$.code").value("999"))
                .andExpect(jsonPath("$.msg").value("user_defined_tag must be not greater than 256."));
    }

    @Test
    public void testPushDownQuery() throws Exception {
        System.setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        System.setProperty("kylin.query.pushdown-enabled", "true");
        System.setProperty("kylin.query.pushdown.cache-enabled", "true");
        System.setProperty("kylin.query.cache-threshold-duration", "0");

        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");

        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("default");

        String queryFileName = "src/test/resources/query/sql_pushdown/query04.sql";
        File sqlFile = new File(queryFileName);
        String sql = KylinTestBase.getTextFromFile(sqlFile);
        sqlRequest.setSql(sql);

        // get h2 query result for comparison
        Statement statement = h2Connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.results[0].length()")
                        .value(resultSet.getMetaData().getColumnCount()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.pushDown").value(true))
                .andDo(MockMvcResultHandlers.print()).andReturn();

        // push down can not hit cache because the cache is expired by default
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.results[0].length()")
                        .value(resultSet.getMetaData().getColumnCount()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.pushDown").value(true))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.engineType").value(QueryContext.PUSHDOWN_RDBMS))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.storageCacheUsed").value(false))
                .andDo(MockMvcResultHandlers.print()).andReturn();

        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.runner-class-name");
        System.clearProperty("kylin.query.pushdown.converter-class-names");
        System.clearProperty("kylin.query.pushdown-enabled");
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
        System.clearProperty("kylin.query.pushdown.cache-enabled");
    }

    @Test
    public void testPrepareQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("default");
        sqlRequest.setSql("SELECT * FROM test_country");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print());

    }

    @Test
    public void testGetMetadata() throws Exception {
        final val mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/query/tables_and_columns").param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        final JsonNode data = jsonNode.get("data");
        Assert.assertTrue(!data.toString().contains(ComputedColumnDesc.getComputedColumnInternalNamePrefix()));
    }
}