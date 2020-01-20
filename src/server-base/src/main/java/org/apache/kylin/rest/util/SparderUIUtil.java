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
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.ui.SparkUI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.jetty.http.HttpHeader;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

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

    private volatile String eraseProxyUriBase;

    public void proxy(HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws IOException {

        URI target = UriComponentsBuilder.fromHttpUrl(getWebUrl())
                .path(servletRequest.getRequestURI().substring(KYLIN_UI_BASE.length()))
                .query(servletRequest.getQueryString()).build(true).toUri();

        final HttpMethod method = HttpMethod.resolve(servletRequest.getMethod());

        try (ClientHttpResponse response = execute(target, method)) {
            rewrite(response, servletResponse, method, servletRequest.getRequestURL(), REDIRECT_THRESHOLD);
        }
    }

    public String getSQLTrackingPath(String id) throws IOException {
        checkVersion();
        return KYLIN_UI_BASE + (Objects.isNull(amSQLBase) ? SQL_EXECUTION_PAGE : amSQLBase) + "?id=" + id;
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

        // reset
        eraseProxyUriBase = null;
        // try to fetch amWebUrl if exists
        try (ClientHttpResponse response = execute(
                UriComponentsBuilder.fromHttpUrl(ui.webUrl()).path(SQL_EXECUTION_PAGE).query("id=1").build().toUri(),
                HttpMethod.GET)) {
            if (HttpStatus.FOUND.equals(response.getStatusCode())) {
                URI uri = response.getHeaders().getLocation();
                amSQLBase = uri.getPath();
                webUrl = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
                appId = ui.appId();
                return;
            }

            // workaround:
            // when driver and rm are at the same machine
            // the AmIpFilter will doFilter incorrectly
            // so we should erase the proxy_uri_base when rewriting the response data.
            final String proxyUriBase = "/proxy/" + ui.appId();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getBody()))) {
                String line;
                while (Objects.nonNull(line = br.readLine())) {
                    if (line.contains(proxyUriBase)) {
                        eraseProxyUriBase = proxyUriBase;
                        break;
                    }
                }
            }

            webUrl = ui.webUrl();
            appId = ui.appId();
        }
    }

    private ClientHttpResponse execute(URI uri, HttpMethod method) throws IOException {
        return factory.createRequest(uri, method).execute();
    }

    private void rewrite(final ClientHttpResponse response, final HttpServletResponse servletResponse,
            final HttpMethod originMethod, final StringBuffer originUrl, final int depth) throws IOException {
        if (depth <= 0) {
            final String msg = String.format("redirect exceed threshold: %d, origin request: [%s %s]",
                    REDIRECT_THRESHOLD, originMethod, originUrl);
            logger.warn("UNEXPECTED_THINGS_HAPPENED {}", msg);
            servletResponse.getWriter().write(msg);
            return;
        }

        if (HttpStatus.FOUND.equals(response.getStatusCode())) {
            try (ClientHttpResponse r = execute(response.getHeaders().getLocation(), originMethod)) {
                rewrite(r, servletResponse, originMethod, originUrl, depth - 1);
            }
            return;
        }

        servletResponse.setStatus(response.getRawStatusCode());

        servletResponse.setHeader(HttpHeader.CONTENT_TYPE.toString(),
                response.getHeaders().getContentType().toString());

        if (response.getHeaders().getContentType().includes(MediaType.IMAGE_PNG)) {
            IOUtils.copy(response.getBody(), servletResponse.getOutputStream());
            return;
        }

        final String fWebUrl = webUrl;
        final String fProxyUriBase = eraseProxyUriBase;
        final PrintWriter writer = servletResponse.getWriter();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getBody()))) {
            String line;
            while (Objects.nonNull(line = br.readLine())) {
                if (Objects.nonNull(fProxyUriBase)) {
                    line = line.replace(fProxyUriBase, "");
                }
                line = line.replace("href=\"/", "href=\"" + KYLIN_UI_BASE + "/");
                line = line.replace("href='/", "href='" + KYLIN_UI_BASE + "/");
                line = line.replace("src=\"/", "src=\"" + KYLIN_UI_BASE + "/");
                line = line.replace("src='/", "src='" + KYLIN_UI_BASE + "/");
                line = line.replace(fWebUrl, KYLIN_UI_BASE);
                writer.write(line);
                writer.println();
            }
        }
    }
}
