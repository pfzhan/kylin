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

import static org.apache.kylin.common.exception.ServerErrorCode.USER_UNAUTHORIZED;

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
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
        if (!KylinConfig.getInstanceFromEnv().isUTEnv() && auth.isAuthenticated()) {
            String name = auth.getName();
            try {
                ManagedUser user = (ManagedUser) userService.loadUserByUsername(name);
                if (user != null && auth instanceof UsernamePasswordAuthenticationToken) {
                    UsernamePasswordAuthenticationToken newAuth = new UsernamePasswordAuthenticationToken(name,
                            auth.getCredentials(), user.getAuthorities());
                    newAuth.setDetails(user);
                    SecurityContextHolder.getContext().setAuthentication(newAuth);
                }
            } catch (Exception e) {
                // user is deleted, return 401 response
                log.debug("Load user by name exception", e);
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                ErrorResponse errorResponse = new ErrorResponse(request.getRequestURL().toString(),
                        new KylinException(USER_UNAUTHORIZED, MsgPicker.getMsg().getINSUFFICIENT_AUTHENTICATION()));
                String errorStr = JsonUtil.writeValueAsIndentString(errorResponse);
                response.setCharacterEncoding("UTF-8");
                PrintWriter writer = response.getWriter();
                writer.print(errorStr);
                writer.flush();
                writer.close();
                return false;
            }
        }
        return true;
    }
}
