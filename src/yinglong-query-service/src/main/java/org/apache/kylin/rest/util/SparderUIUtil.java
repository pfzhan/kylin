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

package org.apache.kylin.rest.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.ui.SparkUI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.jetty.http.HttpHeader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import io.kyligence.kap.common.util.Unsafe;

@Component("sparderUIUtil")
public class SparderUIUtil {

    public static final String UI_BASE = "/sparder";

    private static final Logger logger = LoggerFactory.getLogger(SparderUIUtil.class);

    private static final String KYLIN_UI_BASE = "/kylin" + UI_BASE;

    private static final String SQL_EXECUTION_PAGE = "/SQL/execution/";

    private static final int REDIRECT_THRESHOLD = 5;

    private final HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(
            HttpClientBuilder.create().setMaxConnPerRoute(128).setMaxConnTotal(1024).disableRedirectHandling().build());

    private volatile String webUrl;

    private volatile String amSQLBase;

    private volatile String appId;

    private volatile String proxyBase;

    private volatile String proxyLocationBase = KylinConfig.getInstanceFromEnv().getUIProxyLocation() + UI_BASE;

    public void proxy(HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws IOException {
        final String currentWebUrl = getWebUrl();
        String uriPath = servletRequest.getRequestURI().substring(KYLIN_UI_BASE.length());

        if (!isProxyBaseEnabled()) {
            uriPath = uriPath.replace(proxyBase, "");
        }

        URI target = UriComponentsBuilder.fromHttpUrl(currentWebUrl).path(uriPath)
                .query(servletRequest.getQueryString()).build(true).toUri();

        final HttpMethod method = HttpMethod.resolve(servletRequest.getMethod());

        try (ClientHttpResponse response = execute(target, method)) {
            rewrite(response, servletResponse, method, Unsafe.getUrlFromHttpServletRequest(servletRequest),
                    REDIRECT_THRESHOLD);
        }
    }

    public String getSQLTrackingPath(String id) throws IOException {
        checkVersion();
        return proxyLocationBase + (isProxyBaseEnabled() ? amSQLBase : SQL_EXECUTION_PAGE) + "?id=" + id;
    }

    private boolean isProxyBaseEnabled() {
        return Objects.nonNull(amSQLBase);
    }

    private String getWebUrl() throws IOException {
        checkVersion();
        return webUrl;
    }

    private void checkVersion() throws IOException {

        final SparkContext sc = SparderEnv.getSparkSession().sparkContext();
        final SparkUI ui = sc.ui().get();
        if (ui.appId().equals(appId)) {
            return;
        }

        // KE-12678
        proxyBase = "/proxy/" + ui.appId();

        // reset
        amSQLBase = null;
        // try to fetch amWebUrl if exists
        try (ClientHttpResponse response = execute(
                UriComponentsBuilder.fromHttpUrl(ui.webUrl()).path(SQL_EXECUTION_PAGE).query("id=1").build().toUri(),
                HttpMethod.GET)) {
            if (response.getStatusCode().is3xxRedirection()) {
                URI uri = response.getHeaders().getLocation();
                amSQLBase = Objects.requireNonNull(uri).getPath();
                webUrl = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
                appId = ui.appId();
                return;
            }

            webUrl = ui.webUrl();
            appId = ui.appId();
        }
    }

    private ClientHttpResponse execute(URI uri, HttpMethod method) throws IOException {
        return factory.createRequest(uri, method).execute();
    }

    private void rewrite(final ClientHttpResponse response, final HttpServletResponse servletResponse,
            final HttpMethod originMethod, final String originUrlStr, final int depth) throws IOException {
        if (depth <= 0) {
            final String msg = String.format(Locale.ROOT, "redirect exceed threshold: %d, origin request: [%s %s]",
                    REDIRECT_THRESHOLD, originMethod, originUrlStr);
            logger.warn("UNEXPECTED_THINGS_HAPPENED {}", msg);
            servletResponse.getWriter().write(msg);
            return;
        }

        HttpHeaders headers = response.getHeaders();
        if (response.getStatusCode().is3xxRedirection()) {
            try (ClientHttpResponse r = execute(headers.getLocation(), originMethod)) {
                rewrite(r, servletResponse, originMethod, originUrlStr, depth - 1);
            }
            return;
        }

        servletResponse.setStatus(response.getRawStatusCode());

        servletResponse.setHeader(HttpHeader.CONTENT_TYPE.toString(),
                Objects.requireNonNull(headers.getContentType()).toString());

        if (Objects.requireNonNull(headers.getContentType()).includes(MediaType.IMAGE_PNG)) {
            IOUtils.copy(response.getBody(), servletResponse.getOutputStream());
            return;
        }

        final String fWebUrl = webUrl;
        final PrintWriter writer = servletResponse.getWriter();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(response.getBody(), Charset.defaultCharset()))) {
            String line;
            while (Objects.nonNull(line = br.readLine())) {
                line = line.replace("href=\"/", "href=\"" + proxyLocationBase + "/");
                line = line.replace("href='/", "href='" + proxyLocationBase + "/");
                line = line.replace("src=\"/", "src=\"" + proxyLocationBase + "/");
                line = line.replace("src='/", "src='" + proxyLocationBase + "/");
                line = line.replace("<script>setUIRoot('')</script>",
                        "<script>setUIRoot('" + proxyLocationBase + "')</script>");
                line = line.replace(fWebUrl, proxyLocationBase);
                writer.write(line);
                writer.println();
            }
        }
    }
}
