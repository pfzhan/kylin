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
package io.kyligence.kap.rest.security;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.sparkproject.jetty.servlet.DefaultServlet;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import io.kyligence.kap.common.util.Unsafe;
import lombok.val;

public class FillEmptyAuthorizationFilterTest extends ServiceTestBase {

    @Test
    public void testEmptyAuthorization() throws IOException, ServletException {
        val fillEmptyAuthorizationFilter = new FillEmptyAuthorizationFilter();
        MockHttpServletRequest mockRequest = new MockHttpServletRequest();
        mockRequest.setRequestURI("/api/test");
        mockRequest.setContentType("application/json");
        MutableHttpServletRequest request = new MutableHttpServletRequest(mockRequest);
        MockHttpServletResponse response = new MockHttpServletResponse();
        Unsafe.setProperty("kap.authorization.skip-basic-authorization", "TRUE");
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) {
            }
        });

        fillEmptyAuthorizationFilter.doFilter(request, response, chain);
        Assert.assertTrue(chain.getRequest() instanceof MutableHttpServletRequest);
        MutableHttpServletRequest temp = ((MutableHttpServletRequest) chain.getRequest());
        temp.getHeaderNames();
        Assert.assertEquals("basic MDow", temp.getHeader("Authorization"));
    }

    @Test
    public void testBasicAuthorization() throws IOException, ServletException {
        val fillEmptyAuthorizationFilter = new FillEmptyAuthorizationFilter();
        MockHttpServletRequest mockRequest = new MockHttpServletRequest();
        mockRequest.setRequestURI("/api/test");
        mockRequest.setContentType("application/json");
        Unsafe.setProperty("kap.authorization.skip-basic-authorization", "FALSE");
        MutableHttpServletRequest request = new MutableHttpServletRequest(mockRequest);
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) {
            }
        });

        fillEmptyAuthorizationFilter.doFilter(request, response, chain);
        Assert.assertNull(((MutableHttpServletRequest) chain.getRequest()).getHeader("Authorization"));
    }

}
