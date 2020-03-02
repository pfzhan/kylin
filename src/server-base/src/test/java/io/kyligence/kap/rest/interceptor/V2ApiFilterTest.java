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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.spark_project.jetty.servlet.DefaultServlet;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.metadata.user.ManagedUser;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(Theories.class)
public class V2ApiFilterTest extends ServiceTestBase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        ManagedUser user = new ManagedUser("ADMIN", "ADMIN", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Test
    public void notInWhiteList() throws IOException, ServletException {
        val filter = new V2ApiFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/kylin/api/projects/default/project_config");
        request.addHeader("Accept", HTTP_VND_APACHE_KYLIN_V2_JSON);

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {

        });
        thrown.expect(NotFoundException.class);
        thrown.expectMessage("/kylin/api/projects/default/project_config API of version v2 is no longer supported");
        filter.doFilter(request, response, chain);

    }

    public static @DataPoints List<String> whiteApiList() {
        return Arrays.asList("/kylin/api/user/authentication", "/kylin/api/models", "/kylin/api/cubes",
                "/kylin/api/cubes/some_cube_name", "/kylin/api/cubes/some_cube_name/rebuild",
                "/kylin/api/cubes/some_cube_name/segments", "/kylin/api/projects", "/kylin/api/jobs",
                "/kylin/api/jobs/some_job/resume", "/kylin/api/query", "/kylin/api/kap/user/users",
                "/kylin/api/access/some_user", "/kylin/api/user_group/usersWithGroup",
                "/kylin/api/access/some_type/some_uuid");
    }

    @Theory
    public void inWhiteList(String api) throws IOException, ServletException {
        val filter = new V2ApiFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        log.debug("filter api {}", api);
        request.setRequestURI(api);
        request.addHeader("Accept", HTTP_VND_APACHE_KYLIN_V2_JSON);

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {

        });

        filter.doFilter(request, response, chain);
    }
}