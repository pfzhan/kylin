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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.rest.exception.NotFoundException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(-400)
public class V2ApiFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest servletRequest = (HttpServletRequest) request;
            if (HTTP_VND_APACHE_KYLIN_V2_JSON.equalsIgnoreCase(servletRequest.getHeader("Accept"))) {
                String uri = servletRequest.getRequestURI();
                try {
                    HandlerMapping handlerMapping = this.getRequestMappingHandlerMapping(servletRequest);
                    if (handlerMapping != null) {
                        HandlerExecutionChain handler = handlerMapping.getHandler((HttpServletRequest) request);
                        if (handler == null || handler.getHandler() == null) {
                            throw new NotFoundException(
                                    String.format(Locale.ROOT, "%s API of version v2 is no longer supported", uri));
                        }
                    }
                } catch (Exception e) {
                    log.warn("get hander from request uri {} failed", uri, e);
                    throw new NotFoundException(
                            String.format(Locale.ROOT, "%s API of version v2 is no longer supported", uri));
                }
            }
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // just override it
    }

    private HandlerMapping getRequestMappingHandlerMapping(HttpServletRequest request) {
        ServletContext servletContext = request.getSession().getServletContext();
        if (servletContext == null) {
            return null;
        }
        WebApplicationContext appContext = WebApplicationContextUtils.getWebApplicationContext(servletContext);

        Map<String, HandlerMapping> allRequestMappings = BeanFactoryUtils.beansOfTypeIncludingAncestors(appContext,
                HandlerMapping.class, true, false);

        return allRequestMappings.values().stream()
                .filter(handlerMapping -> handlerMapping.getClass().equals(RequestMappingHandlerMapping.class))
                .findAny().orElse(null);
    }
}
