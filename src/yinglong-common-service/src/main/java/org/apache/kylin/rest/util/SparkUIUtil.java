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
package org.apache.kylin.rest.util;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Locale;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.util.UriComponentsBuilder;

import io.kyligence.kap.common.util.Unsafe;
import java.util.Objects;

public class SparkUIUtil {

    private static final HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(
            HttpClientBuilder.create().setMaxConnPerRoute(128).setMaxConnTotal(1024).disableRedirectHandling().build());
    private static final Logger logger = LoggerFactory.getLogger(SparkUIUtil.class);

    private static final int REDIRECT_THRESHOLD = 5;

    private static final String SPARK_UI_PROXY_HEADER = "X-Kylin-Proxy-Path";

    private SparkUIUtil() {
    }

    public static void resendSparkUIRequest(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
            String sparkUiUrl, String uriPath, String proxyLocationBase) throws IOException {
        URI target = UriComponentsBuilder.fromHttpUrl(sparkUiUrl).path(uriPath).query(servletRequest.getQueryString())
                .build(true).toUri();

        final HttpMethod method = HttpMethod.resolve(servletRequest.getMethod());

        try (ClientHttpResponse response = execute(target, method, proxyLocationBase)) {
            rewrite(response, servletResponse, method, Unsafe.getUrlFromHttpServletRequest(servletRequest),
                    REDIRECT_THRESHOLD, proxyLocationBase);
        }
    }

    public static ClientHttpResponse execute(URI uri, HttpMethod method, String proxyLocationBase) throws IOException {
        ClientHttpRequest clientHttpRequest = factory.createRequest(uri, method);
        clientHttpRequest.getHeaders().put(SPARK_UI_PROXY_HEADER, Arrays.asList(proxyLocationBase));
        return clientHttpRequest.execute();
    }

    private static void rewrite(final ClientHttpResponse response, final HttpServletResponse servletResponse,
            final HttpMethod originMethod, final String originUrlStr, final int depth, String proxyLocationBase)
            throws IOException {
        if (depth <= 0) {
            final String msg = String.format(Locale.ROOT, "redirect exceed threshold: %d, origin request: [%s %s]",
                    REDIRECT_THRESHOLD, originMethod, originUrlStr);
            logger.warn("UNEXPECTED_THINGS_HAPPENED {}", msg);
            servletResponse.getWriter().write(msg);
            return;
        }
        HttpHeaders headers = response.getHeaders();
        if (response.getStatusCode().is3xxRedirection()) {
            try (ClientHttpResponse r = execute(headers.getLocation(), originMethod, proxyLocationBase)) {
                rewrite(r, servletResponse, originMethod, originUrlStr, depth - 1, proxyLocationBase);
            }
            return;
        }

        servletResponse.setStatus(response.getRawStatusCode());

        if (response.getHeaders().getContentType() != null) {
            servletResponse.setHeader(HttpHeaders.CONTENT_TYPE,
                    Objects.requireNonNull(headers.getContentType()).toString());
        }
        IOUtils.copy(response.getBody(), servletResponse.getOutputStream());

    }

}
