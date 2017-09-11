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

package io.kyligence.kap.rest.spring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.apache.commons.io.input.TeeInputStream;
import org.bouncycastle.util.io.TeeOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.DelegatingServletInputStream;
import org.springframework.mock.web.DelegatingServletOutputStream;

import com.google.common.base.Strings;

public class RequestResponseLoggingFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(RequestResponseLoggingFilter.class);
    private RequestLoggingWrapper servletRequestWrapper;
    private ResponseLoggingWrapper servletResponseWrapper;

    public RequestResponseLoggingFilter() {
        logger.debug("init");
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        logger.debug("doFilter");
        servletRequestWrapper = new RequestLoggingWrapper((HttpServletRequest) request);
        servletResponseWrapper = new ResponseLoggingWrapper((HttpServletResponse) response);
        chain.doFilter(servletRequestWrapper, servletResponseWrapper);

        String requestBody = new String(servletRequestWrapper.getBaos().toByteArray());
        String responseBody = new String(servletResponseWrapper.getBaos().toByteArray());

        // Log request
        logger.debug("REQUEST METHOD: " + servletRequestWrapper.getMethod());
        logger.debug("REQUEST URI: " + servletRequestWrapper.getRequestURI());
        logger.debug("REQUEST HEADERS:");
        Enumeration<String> headerNames = servletRequestWrapper.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String name = headerNames.nextElement();
            logger.debug("\t" + name + " : " + servletRequestWrapper.getHeader(name));
        }

        if (!Strings.isNullOrEmpty(requestBody)) {
            logger.debug("REQUEST BODY: " + requestBody);
        }

        // Log response
        logger.debug("RESPONSE HTTP CODE: " + servletResponseWrapper.getStatus());
        if (!Strings.isNullOrEmpty(responseBody)) {
            logger.debug("RESPONSE BODY: " + responseBody);
        }
    }

    @Override
    public void destroy() {
    }

    private class RequestLoggingWrapper extends HttpServletRequestWrapper {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        /**
         * Constructs a request object wrapping the given request.
         *
         * @param request
         * @throws IllegalArgumentException if the request is null
         */
        public RequestLoggingWrapper(HttpServletRequest request) {
            super(request);
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            return new DelegatingServletInputStream(new TeeInputStream(super.getInputStream(), baos));
        }

        public ByteArrayOutputStream getBaos() {
            return baos;
        }
    }

    private class ResponseLoggingWrapper extends HttpServletResponseWrapper {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        /**
         * Constructs a response adaptor wrapping the given response.
         *
         * @param response
         * @throws IllegalArgumentException if the response is null
         */
        public ResponseLoggingWrapper(HttpServletResponse response) {
            super(response);
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return new DelegatingServletOutputStream(new TeeOutputStream(super.getOutputStream(), baos));
        }

        public ByteArrayOutputStream getBaos() {
            return baos;
        }
    }
}
