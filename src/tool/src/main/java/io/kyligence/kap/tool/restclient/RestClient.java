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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.tool.restclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;

/**
 */
public class RestClient {

    private static final Logger logger = LoggerFactory.getLogger(RestClient.class);

    protected static Pattern fullRestPattern = Pattern.compile("(?:([^:]+)[:]([^@]+)[@])?([^:]+)(?:[:](\\d+))?");

    private static final int HTTP_CONNECTION_TIMEOUT_MS = 30000;
    private static final int HTTP_SOCKET_TIMEOUT_MS = 120000;

    public static final String SCHEME_HTTP = "http://";
    private static final String ROUTED = "routed";
    public static final String KYLIN_API_PATH = "/kylin/api";

    public static boolean matchFullRestPattern(String uri) {
        Matcher m = fullRestPattern.matcher(uri);
        return m.matches();
    }

    // ============================================================================

    protected String host;
    protected int port;
    protected String baseUrl;
    protected String userName;
    protected String password;
    protected DefaultHttpClient client;

    /**
     * @param uri "user:pwd@host:port"
     */
    public RestClient(String uri) {
        Matcher m = fullRestPattern.matcher(uri);
        if (!m.matches())
            throw new IllegalArgumentException("URI: " + uri + " -- does not match pattern " + fullRestPattern);

        String user = m.group(1);
        String pwd = m.group(2);
        String host = m.group(3);
        String portStr = m.group(4);
        int port = Integer.parseInt(portStr == null ? "7070" : portStr);

        init(host, port, user, pwd);
    }

    public RestClient(String host, int port, String userName, String password) {
        init(host, port, userName, password);
    }

    private void init(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = SCHEME_HTTP + host + ":" + port + KYLIN_API_PATH;

        final HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setSoTimeout(httpParams, HTTP_SOCKET_TIMEOUT_MS);
        HttpConnectionParams.setConnectionTimeout(httpParams, HTTP_CONNECTION_TIMEOUT_MS);

        final PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        cm.setDefaultMaxPerRoute(config.getRestClientDefaultMaxPerRoute());
        cm.setMaxTotal(config.getRestClientMaxTotal());

        client = new DefaultHttpClient(cm, httpParams);

        if (userName != null && password != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            client.setCredentialsProvider(provider);
        }
    }

    public HttpResponse query(String sql, String project) throws IOException {
        String url = baseUrl + "/query";
        HttpPost post = newPost(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("sql", sql);
        paraMap.put("project", project);
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        post.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = client.execute(post);
        return response;
    }

    public HttpResponse updateUser(Object object) throws IOException {
        String url = baseUrl + "/user/update_user";
        HttpPost post = newPost(url);
        post.addHeader(ROUTED, "true");
        String jsonMsg = JsonUtil.writeValueAsIndentString(object);
        post.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = null;
        try {
            response = client.execute(post);
            if (response.getStatusLine().getStatusCode() != 200) {
                String msg = EntityUtils.toString(response.getEntity());
                logger.error("Invalid response " + response.getStatusLine().getStatusCode() + " with update user " + url
                        + "\n" + msg);
            }
        } finally {
            cleanup(post, response);
            tryCatchUp();
        }
        return response;
    }

    public HttpResponse notify(BroadcastEventReadyNotifier notifier) throws IOException {
        String url = baseUrl + "/broadcast";
        HttpPost post = newPost(url);
        post.addHeader(ROUTED, "true");
        HttpResponse response = null;
        try {
            post.setEntity(new ByteArrayEntity(JsonUtil.writeValueAsBytes(notifier), ContentType.APPLICATION_JSON));
            response = client.execute(post);
            if (response.getStatusLine().getStatusCode() != 200) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new KylinException(CommonErrorCode.FAILED_NOTIFY_CATCHUP, "Invalid response "
                        + response.getStatusLine().getStatusCode() + " with notify catch up url " + url + "\n" + msg);
            }
        } finally {
            cleanup(post, response);
        }
        return response;
    }

    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Content-Type", "application/json");
    }

    private HttpPost newPost(String url) {
        HttpPost post = new HttpPost(url);
        addHttpHeaders(post);
        return post;
    }

    private HttpPut newPut(String url) {
        HttpPut put = new HttpPut(url);
        addHttpHeaders(put);
        return put;
    }

    private String getContent(HttpResponse response) throws IOException {
        StringBuilder result = new StringBuilder();
        try (InputStreamReader reader = new InputStreamReader(response.getEntity().getContent(),
                Charset.defaultCharset()); BufferedReader rd = new BufferedReader(reader)) {
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        }
        return result.toString();
    }

    private void cleanup(HttpRequestBase request, HttpResponse response) {
        try {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        } catch (Exception ex) {
            logger.error("Error during HTTP connection cleanup", ex);
        }
        request.releaseConnection();
    }

    public <T> T getKapHealthStatus(TypeReference<T> clz, byte[] encryptedToken) throws Exception {
        String url = baseUrl + "/health/instance_info";

        HttpPost httpPost = new HttpPost(url);
        httpPost.setEntity(new ByteArrayEntity(encryptedToken));
        HttpResponse response = null;
        try {
            httpPost.setURI(new URI(url));
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with health status url " + url + "\n" + msg);
            }

            return JsonUtil.readValue(getContent(response), clz);
        } finally {
            cleanup(httpPost, response);
        }
    }

    public void downOrUpGradeKE(String status, byte[] encryptedToken) throws Exception {
        String url = baseUrl + "/health/instance_service/" + status;

        HttpPost httpPost = new HttpPost(url);
        httpPost.setEntity(new ByteArrayEntity(encryptedToken));
        HttpResponse response = null;
        try {
            httpPost.setURI(new URI(url));
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with downOrUpGradeKE url " + url + "\n" + msg);
            }
        } finally {
            cleanup(httpPost, response);
        }
    }

    public boolean updateDiagProgress(String diagId, String stage, float progress) {
        String url = baseUrl + "/system/diag/progress";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            HashMap<String, Object> paraMap = Maps.newHashMap();
            paraMap.put("diag_id", diagId);
            paraMap.put("stage", stage);
            paraMap.put("progress", progress);
            put.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(paraMap), "UTF-8"));
            response = client.execute(put);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                logger.warn("Invalid response {} with updateDiagProgress url: {}\n{}",
                        response.getStatusLine().getStatusCode(), url, msg);
                return false;
            }
        } catch (Exception e) {
            logger.warn("Error during update diag progress", e);
        } finally {
            cleanup(put, response);
        }
        return true;
    }

    public boolean rollUpEventLog() {
        String url = baseUrl + "/system/roll_event_log";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            response = client.execute(put);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                logger.warn("Invalid response {}  rollup event_log url: {}\n{}",
                        response.getStatusLine().getStatusCode(), url, msg);
                return false;
            }
        } catch (Exception e) {
            logger.warn("Error during get rollup event_log");
        } finally {
            cleanup(put, response);
        }
        return true;
    }

    private void tryCatchUp() {
        try {
            ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.getAuditLogStore().catchupWithTimeout();
        } catch (Exception e) {
            logger.error("Failed to catchup manually.", e);
        }
    }

}
