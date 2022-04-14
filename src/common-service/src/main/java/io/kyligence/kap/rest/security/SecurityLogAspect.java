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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import io.kyligence.kap.rest.util.SecurityLoggerUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class SecurityLogAspect {

    @Autowired
    private LoginLogFilter loginLogFilter;

    @AfterReturning("(execution(* io.kyligence.kap.rest.security.LimitLoginAuthenticationProvider.authenticate(..)) "
            + "|| execution(* io.kyligence.kap.rest.security.OpenAuthenticationProvider.authenticate(..))"
            + "|| execution(* io.kyligence.kap.rest.security.LdapAuthenticationProvider.authenticate(..))) "
            + "&& args(authentication)")
    public void doAfterLoginSuccess(Authentication authentication) {
        if (null == loginLogFilter.getLoginInfoThreadLocal().get()) {
            loginLogFilter.getLoginInfoThreadLocal().set(new LoginLogFilter.LoginInfo());
        }

        loginLogFilter.getLoginInfoThreadLocal().get().setUserName(authentication.getName());
        loginLogFilter.getLoginInfoThreadLocal().get().setLoginSuccess(Boolean.TRUE);

    }

    @AfterThrowing(pointcut = "(execution(* io.kyligence.kap.rest.security.LimitLoginAuthenticationProvider.authenticate(..)) "
            + "|| execution(* io.kyligence.kap.rest.security.OpenAuthenticationProvider.authenticate(..))"
            + "|| execution(* io.kyligence.kap.rest.security.LdapAuthenticationProvider.authenticate(..))) "
            + "&& args(authentication)", throwing = "exception")
    public void doAfterLoginError(Authentication authentication, Exception exception) {
        if (null == loginLogFilter.getLoginInfoThreadLocal().get()) {
            loginLogFilter.getLoginInfoThreadLocal().set(new LoginLogFilter.LoginInfo());
        }

        loginLogFilter.getLoginInfoThreadLocal().get().setUserName(authentication.getName());
        loginLogFilter.getLoginInfoThreadLocal().get().setLoginSuccess(Boolean.FALSE);
        loginLogFilter.getLoginInfoThreadLocal().get().setException(exception);
    }

    @AfterReturning(value = "execution(* org.springframework.security.web.authentication.logout.LogoutSuccessHandler.onLogoutSuccess(..)) "
            + "&& args(request, response, authentication)", argNames = "request, response, authentication")
    public void doAfterLogoutSuccess(HttpServletRequest request, HttpServletResponse response,
            Authentication authentication) {
        SecurityLoggerUtils.recordLogout(authentication.getName());
    }
}
