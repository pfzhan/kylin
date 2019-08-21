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
package io.kyligence.kap.rest.interceptor;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.project.NProjectLoader;
import lombok.Data;
import lombok.Getter;
import lombok.val;

@Component
@Order(1)
public class RepeatableRequestBodyFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        try {
            String project = request.getParameter("project");
            ServletRequest requestWrapper = request;
            if (StringUtils.isEmpty(project) && request.getContentType() != null
                    && request.getContentType().contains("json")) {
                requestWrapper = new RepeatableBodyRequestWrapper((HttpServletRequest) request);
                try {
                    val projectRequest = JsonUtil.readValue(((RepeatableBodyRequestWrapper) requestWrapper).getBody(),
                            ProjectRequest.class);
                    if (projectRequest != null) {
                        project = projectRequest.getProject();
                    }
                } catch (IOException ignored) {
                    // ignore JSON exception
                }
            }
            NProjectLoader.updateCache(project);
            chain.doFilter(requestWrapper, response);
        } finally {
            NProjectLoader.removeCache();
        }
    }

    @Override
    public void destroy() {
        // just override it
    }

    public static class RepeatableBodyRequestWrapper extends HttpServletRequestWrapper {

        @Getter
        private final String body;

        RepeatableBodyRequestWrapper(HttpServletRequest request) throws IOException {
            super(request);
            body = IOUtils.toString(request.getInputStream());
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body.getBytes());
            return new ServletInputStream() {
                public int read() throws IOException {
                    return byteArrayInputStream.read();
                }
            };
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return new BufferedReader(new InputStreamReader(this.getInputStream()));
        }

    }

    @Data
    public static class ProjectRequest {

        private String project;

    }
}
