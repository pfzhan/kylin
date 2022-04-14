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

import io.kyligence.kap.rest.util.SecurityLoggerUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.session.web.http.SaveSessionException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE+1)
public class LoginLogFilter extends OncePerRequestFilter {

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LoginInfo {
        private String userName;
        private Boolean loginSuccess;
        private Exception exception;
    }

    @Getter
    private ThreadLocal<LoginInfo> loginInfoThreadLocal = new ThreadLocal<>();

    @Override
    protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
            FilterChain filterChain) throws ServletException, IOException {
        try {
            filterChain.doFilter(httpServletRequest, httpServletResponse);
        } catch (SaveSessionException e) {
            LoginInfo loginInfo = getLoginInfoThreadLocal().get();
            if (null == loginInfo) {
                loginInfo = new LoginInfo();
                loginInfo.setUserName("anonymous");
                getLoginInfoThreadLocal().set(loginInfo);
            }
            loginInfo.setLoginSuccess(Boolean.FALSE);
            loginInfo.setException(e);
            throw e;
        } finally {
            LoginInfo loginInfo = getLoginInfoThreadLocal().get();
            if (null != loginInfo) {
                try {
                    if (loginInfo.getLoginSuccess()) {
                        SecurityLoggerUtils.recordLoginSuccess(loginInfo.getUserName());
                    } else {
                        SecurityLoggerUtils.recordLoginFailed(loginInfo.getUserName(), loginInfo.getException());
                    }
                } catch (Exception e) {
                    logger.error("Failed to log the login status!", e);
                } finally {
                    getLoginInfoThreadLocal().remove();
                }
            }
        }
    }
}
