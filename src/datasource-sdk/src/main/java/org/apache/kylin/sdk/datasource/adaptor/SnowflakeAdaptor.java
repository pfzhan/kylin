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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.CachedRowSet;

import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.sdk.datasource.adaptor.response.KylinSnowflakeResponse;

import com.google.common.base.Preconditions;

/**
 * Known limitation:
 * 1.dayofweek different answer
 * 2.cast as integer different answer
 */
public class SnowflakeAdaptor extends DefaultAdaptor {

    public static final String AUTHENTICATOR = "kylin.source.authenticator";
    public static final String OAUTH_ENABLED = "snowflake.oauth.enabled";
    public static final String OAUTH_TOKEN_URL = "snowflake.oauth.token.url";
    public static final String AAD_CLIENT_ID = "snowflake.aad.client.id";
    public static final String AAD_CLIENT_SECRET = "snowflake.aad.client.secret";
    public static final String AAD_SCOPE = "snowflake.aad.scope";
    public static final String AAD_GRANT_TYPE = "snowflake.aad.grant.type";

    private static Pattern patternSubstr = Pattern.compile("SUBSTRING\\(([^,]*)\\)");
    private Pattern patternTrim = Pattern.compile("TRIM\\(.*BOTH.*FROM\\s+(.+)\\)");

    public SnowflakeAdaptor(AdaptorConfig config) throws Exception {
        super(config);
        initAADParams();
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return super.getConnection();
        } catch (SnowflakeSQLException e) {
            initAADParams();
        }

        return super.getConnection();
    }

    @Override
    public String fixSql(String sql) {
        sql = tryReplaceBackTick(sql);
        sql = fixSubstringFromFor(sql);
        sql = convertTrim(sql);
        sql = rmAsyncMetric(sql);
        return sql;
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = getConnection()) {
            String database = con.getCatalog();
            Preconditions.checkArgument(StringUtils.isNotEmpty(database),
                    "Snowflake needs a specific database in connection string.");

            try (ResultSet rs = con.getMetaData().getSchemas(database, "%")) {
                String schema;
                String catalog;
                while (rs.next()) {
                    schema = rs.getString("TABLE_SCHEM");
                    catalog = rs.getString("TABLE_CATALOG");
                    // Skip system schemas
                    if (database.equals(catalog) && !schema.equals("INFORMATION_SCHEMA")) {
                        ret.add(schema);
                    }
                }
            }
        }
        return ret;
    }

    @Override
    public List<String> listTables(String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection con = getConnection()) {
            String database = con.getCatalog();

            try (ResultSet rs = con.getMetaData().getTables(database, schema, null, null)) {
                while (rs.next()) {
                    String table = rs.getString("TABLE_NAME");
                    tables.add(table);
                }
            }
        }
        return tables;
    }

    @Override
    public CachedRowSet getTableColumns(String schema, String table) throws SQLException {
        try (Connection conn = getConnection()) {
            String catalog = conn.getCatalog();
            try (ResultSet rs = conn.getMetaData().getColumns(catalog, schema, table, null)) {
                return cacheResultSet(rs);
            }
        }
    }

    protected void initAADParams() {
        Map<String, String> options = config.getOptions();
        if (options.get(OAUTH_ENABLED).equals("true")) {
            dataSource.addConnectionProperty("authenticator", options.get(AUTHENTICATOR));
            String url = options.get(OAUTH_TOKEN_URL);
            String clientId = options.get(AAD_CLIENT_ID);
            String clientSecret = options.get(AAD_CLIENT_SECRET);
            String scope = options.get(AAD_SCOPE);
            String grantType = options.get(AAD_GRANT_TYPE);
            KylinSnowflakeResponse response = requestSnowflakeToken(url, clientId, clientSecret, grantType, scope);
            dataSource.addConnectionProperty("token", response.getAccessToken());
        }
    }

    private String tryReplaceBackTick(String sql) {
        return sql.replace("`", "\"");
    }

    private String fixSubstringFromFor(String sql) {
        String sqlReturn = sql;
        Matcher matcher = patternSubstr.matcher(sql);
        while (matcher.find()) {
            String originSubStr = matcher.group(1);
            String fixSubStr = matcher.group(1).replace(" FROM ", " , ").replace(" FOR ", " , ");
            sqlReturn = sqlReturn.replace(originSubStr, fixSubStr);
        }
        return sqlReturn;
    }

    private String convertTrim(String sql) {
        String sqlReturn = sql;
        Matcher matcher = patternTrim.matcher(sql);
        boolean isFind = matcher.find();
        if (isFind) {
            String originStr = matcher.group(0);
            String fixStr = "TRIM(" + matcher.group(1) + ")";
            sqlReturn = sqlReturn.replace(originStr, fixStr);
        }
        return sqlReturn;
    }

    private String rmAsyncMetric(String sql) {
        return sql.replaceAll("ASYMMETRIC", "");
    }

    private KylinSnowflakeResponse requestSnowflakeToken(String url, String clientId, String clientSecret,
            String grantType, String scope) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");

            List<NameValuePair> parameters = new ArrayList<>(10);
            parameters.add(new BasicNameValuePair("client_id", clientId));
            parameters.add(new BasicNameValuePair("client_secret", clientSecret));
            parameters.add(new BasicNameValuePair("grant_type", grantType));
            parameters.add(new BasicNameValuePair("scope", scope));
            httpPost.setEntity(new UrlEncodedFormEntity(parameters));
            CloseableHttpResponse response = httpclient.execute(httpPost);

            if (response.getStatusLine().getStatusCode() == 200) {
                return JsonUtil.readValue(response.getEntity().getContent(), KylinSnowflakeResponse.class);
            }
        } catch (Exception e) {
            throw new IllegalStateException("request snowflake token exception", e);
        }

        return null;
    }
}
