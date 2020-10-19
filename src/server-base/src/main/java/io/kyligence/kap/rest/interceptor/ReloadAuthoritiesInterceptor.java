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

import java.util.Collections;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import io.kyligence.kap.metadata.user.ManagedUser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(-300)
public class ReloadAuthoritiesInterceptor extends HandlerInterceptorAdapter {

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        if (!KylinConfig.getInstanceFromEnv().isUTEnv() && auth != null
                && !(auth instanceof AnonymousAuthenticationToken) && auth.isAuthenticated()) {
            String name = auth.getName();
            ManagedUser user = null;
            try {
                user = (ManagedUser) userService.loadUserByUsername(name);
            } catch (UsernameNotFoundException e) {
                // can not get user by name
                log.debug("Load user by name exception, set authentication to AnonymousAuthenticationToken", e);
                SecurityContextHolder.getContext().setAuthentication(new AnonymousAuthenticationToken("anonymousUser",
                        "anonymousUser", Collections.singletonList(new SimpleGrantedAuthority("ROLE_ANONYMOUS"))));
                return true;
            }

            if (user != null && auth instanceof UsernamePasswordAuthenticationToken) {
                UsernamePasswordAuthenticationToken newAuth = new UsernamePasswordAuthenticationToken(name,
                        auth.getCredentials(), user.getAuthorities());
                newAuth.setDetails(user);
                SecurityContextHolder.getContext().setAuthentication(newAuth);
            }
        }
        return true;
    }
}
