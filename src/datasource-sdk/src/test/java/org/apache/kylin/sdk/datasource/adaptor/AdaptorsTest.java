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
package org.apache.kylin.sdk.datasource.adaptor;

import static org.powermock.api.mockito.PowerMockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicStatusLine;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.sdk.datasource.adaptor.response.KylinSnowflakeResponse;
import org.apache.kylin.sdk.datasource.framework.JdbcConnectorTest;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import lombok.SneakyThrows;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpClients.class, UserGroupInformation.class})
public class AdaptorsTest extends JdbcConnectorTest {

    @BeforeClass
    public static void setup() throws Exception {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);
    }

    @Test
    public void testMySQLAdaptor() throws Exception {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.MysqlAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "mysql");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertFalse(connector.listDatabases().contains("EDW"));
        Assert.assertTrue(connector.listTables("DEFAULT").isEmpty());
        Assert.assertFalse(connector.listTables("DB").isEmpty());
        Assert.assertNotNull(connector.getTable("DB", "ROLES"));
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }

    @Test
    public void testMssqlAdaptor() throws SQLException {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.MssqlAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "mssql");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        AbstractJdbcAdaptor adaptor = connector.getAdaptor();
        Assert.assertEquals("select 1", adaptor.fixSql("select 1"));
        Assert.assertEquals("float", adaptor.toSourceTypeName("DOUBLE"));
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertNotNull(connector.getTable("DB", "ROLES"));
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }

    @Test
    public void testSnowflakeAdaptor() throws SQLException {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.SnowflakeAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "snowflake");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        AbstractJdbcAdaptor adaptor = connector.getAdaptor();
        Assert.assertEquals("select 1", adaptor.fixSql("select 1"));
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertFalse(connector.listTables("DEFAULT").isEmpty());
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }

    @Test
    public void testSnowflakeTokenAdaptor() throws SQLException, IOException {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.SnowflakeAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "snowflake");

        Map<String, String> options = new HashMap<>();
        options.put("snowflake.oauth.enabled", "true");
        options.put("kylin.source.authenticator", "?");
        options.put(SnowflakeAdaptor.OAUTH_TOKEN_URL, "https://login.microsoftonline.com/111/oauth2/v2.0/token");
        options.put(SnowflakeAdaptor.AAD_CLIENT_ID, "73918259-8af4-46b0-a514-1116");
        options.put(SnowflakeAdaptor.AAD_CLIENT_SECRET, "11111");
        options.put(SnowflakeAdaptor.AAD_SCOPE, "https://8b66b074-2767-4f10-111/.default");
        options.put(SnowflakeAdaptor.AAD_GRANT_TYPE, "client_credentials");

        PowerMockito.mockStatic(HttpClients.class);
        CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);

        when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

        when(httpResponse.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, ""));
        final KylinSnowflakeResponse snowflakeResponse = new KylinSnowflakeResponse();
        snowflakeResponse.setAccessToken("111111");
        when(httpResponse.getEntity()).thenReturn(new BasicHttpEntity() {
            @SneakyThrows
            @Override
            public InputStream getContent() throws IllegalStateException {
                return new ByteArrayInputStream(JsonUtil.writeValueAsIndentBytes(snowflakeResponse));
            }
        });
        when(HttpClients.createDefault()).thenReturn(httpClient);


        KylinConfigExt kylinConfigExt = KylinConfigExt.createInstance(getTestConfig(), options);
        connector = SourceConnectorFactory.getJdbcConnector(kylinConfigExt);
        AbstractJdbcAdaptor adaptor = connector.getAdaptor();
        Assert.assertEquals("select 1", adaptor.fixSql("select 1"));
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertFalse(connector.listTables("DEFAULT").isEmpty());
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }

    @Test
    public void testSQLDWAdaptor() throws SQLException {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.SQLDWAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "sqldw");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }


}
