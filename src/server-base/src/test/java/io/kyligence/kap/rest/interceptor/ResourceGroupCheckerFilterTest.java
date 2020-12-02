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

import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import lombok.val;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.spark_project.jetty.servlet.DefaultServlet;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

public class ResourceGroupCheckerFilterTest extends ServiceTestBase {
    @Test
    public void testResourceGroupDisabled() throws IOException, ServletException {
        val filter = new ResourceGroupCheckerFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/api/test");
        request.setContentType("application/json");
        request.setContent(("" +
                "{\n" +
                "    \"project\": \"a\"" +
                "}").getBytes());

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
                val manager = ResourceGroupManager.getInstance(getTestConfig());
                Assert.assertFalse(manager.isResourceGroupEnabled());
            }
        });

        filter.doFilter(request, response, chain);
        Assert.assertNull(request.getAttribute("error"));
    }

    @Test
    public void testProjectWithoutResourceGroupException() throws IOException, ServletException {
        setResourceGroupEnabled();

        val filter = new ResourceGroupCheckerFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/api/test");
        request.setContentType("application/json");
        request.setContent(("" +
                "{\n" +
                "    \"project\": \"a\"" +
                "}").getBytes());

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
                val manager = ResourceGroupManager.getInstance(getTestConfig());
                Assert.assertTrue(manager.isResourceGroupEnabled());
            }
        });

        filter.doFilter(request, response, chain);
        Assert.assertNotNull(request.getAttribute("error"));
    }

    @Test
    public void testWhitelistApi() throws IOException, ServletException {
        setResourceGroupEnabled();

        val filter = new ResourceGroupCheckerFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setMethod("GET");
        request.setRequestURI("/kylin/api/projects");
        request.setContentType("application/json");
        request.setContent(("" +
                "{\n" +
                "    \"project\": \"a\"" +
                "}").getBytes());

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
                val manager = ResourceGroupManager.getInstance(getTestConfig());
                Assert.assertTrue(manager.isResourceGroupEnabled());
            }
        });

        filter.doFilter(request, response, chain);
        Assert.assertNull(request.getAttribute("error"));
    }


    private void setResourceGroupEnabled() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.getResourceGroup();
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(true);
        });
    }
}
