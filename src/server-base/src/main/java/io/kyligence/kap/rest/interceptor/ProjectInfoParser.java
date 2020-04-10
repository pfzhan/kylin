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

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.Data;
import lombok.Getter;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class ProjectInfoParser implements IKeep{

    public static Pair<String, ServletRequest> parseProjectInfo(ServletRequest request) {
        ServletRequest requestWrapper = request;
        String project = null;
        try {
            requestWrapper = new RepeatableBodyRequestWrapper((HttpServletRequest) request);
            project = requestWrapper.getParameter("project");
            if (StringUtils.isEmpty(project) && request.getContentType() != null
                    && request.getContentType().contains("json")) {
                val projectRequest = JsonUtil.readValue(((RepeatableBodyRequestWrapper) requestWrapper).getBody(),
                        ProjectRequest.class);
                if (projectRequest != null) {
                    project = projectRequest.getProject();
                }

            }

            if (StringUtils.isEmpty(project)) {
                project = UnitOfWork.GLOBAL_UNIT;
            }
        } catch (IOException e) {
            // ignore JSON exception
        }
        return new Pair<>(project, requestWrapper);
    }

    public static class RepeatableBodyRequestWrapper extends HttpServletRequestWrapper {

        @Getter
        private final String body;

        public RepeatableBodyRequestWrapper(HttpServletRequest request) throws IOException {
            super(request);
            body = IOUtils.toString(request.getInputStream());
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body.getBytes());
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
            return new BufferedReader(new InputStreamReader(this.getInputStream()));
        }

    }

    @Data
    public static class ProjectRequest implements IKeep {

        private String project;

    }
}
