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
import java.nio.charset.Charset;
import java.util.HashMap;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.glassfish.jersey.uri.UriTemplate;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.rest.constant.ProjectInfoParserConstant;
import lombok.Data;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectInfoParser {

    private static final String PROJECT_PARAM = "project";

    private ProjectInfoParser() {
        throw new IllegalStateException("Utility class");
    }

    public static Pair<String, HttpServletRequest> parseProjectInfo(HttpServletRequest request) {
        HttpServletRequest requestWrapper = request;
        String project = null;
        try {
            String contentType = request.getContentType();
            if (contentType != null && contentType.contains("application/x-www-form-urlencoded")) {
                // parse parameter and load form from body
                project = requestWrapper.getParameter(PROJECT_PARAM);
            }

            requestWrapper = new RepeatableBodyRequestWrapper(request);
            project = requestWrapper.getParameter(PROJECT_PARAM);
            if (StringUtils.isEmpty(project) && contentType != null
                    && contentType.contains("json")) {
                val projectRequest = JsonUtil.readValue(((RepeatableBodyRequestWrapper) requestWrapper).getBody(),
                        ProjectRequest.class);
                if (projectRequest != null) {
                    project = projectRequest.getProject();
                }

            }
        } catch (IOException e) {
            // ignore JSON exception
        }

        if (StringUtils.isEmpty(project)) {
            project = extractProject((request).getRequestURI());
        }

        if (StringUtils.isEmpty(project)) {
            project = UnitOfWork.GLOBAL_UNIT;
        }

        log.debug("Parsed project {} from request {}", project, (request).getRequestURI());
        return new Pair<>(project, requestWrapper);
    }

    public static class RepeatableBodyRequestWrapper extends HttpServletRequestWrapper {

        @Getter
        private final byte[] body;

        public RepeatableBodyRequestWrapper(HttpServletRequest request) throws IOException {
            super(request);
            body = IOUtils.toByteArray(request.getInputStream());
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body);
            return new ServletInputStream() {

                @Override
                public boolean isFinished() {
                    return isFinished;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {
                    // Do not support it
                }

                private boolean isFinished;

                public int read() throws IOException {
                    int b = byteArrayInputStream.read();
                    isFinished = b == -1;
                    return b;
                }

            };
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return new BufferedReader(new InputStreamReader(this.getInputStream(), Charset.defaultCharset()));
        }

    }

    @Data
    public static class ProjectRequest {
        private String project;
    }

    static String extractProject(String url) {
        for (String needParserURI : ProjectInfoParserConstant.INSTANCE.PROJECT_PARSER_URI_LIST) {
            val uriTemplate = new UriTemplate(needParserURI);
            val kvMap = new HashMap<String, String>();

            if (uriTemplate.match(url, kvMap)) {
                return kvMap.get(PROJECT_PARAM);
            }
        }

        return null;
    }
}
