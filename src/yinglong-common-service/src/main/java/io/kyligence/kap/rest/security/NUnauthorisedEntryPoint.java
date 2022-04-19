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

import static org.apache.kylin.common.exception.ServerErrorCode.LOGIN_FAILED;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_DATA_SOURCE_CONNECTION_FAILED;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_LOCKED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_UNAUTHORIZED;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.util.Optional;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.http.MediaType;
import org.springframework.ldap.CommunicationException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.util.Unsafe;

@Component(value = "nUnauthorisedEntryPoint")
public class NUnauthorisedEntryPoint implements AuthenticationEntryPoint {

    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException exception)
            throws IOException, ServletException {
        if (exception instanceof LockedException) {
            setErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST,
                    new KylinException(USER_LOCKED, exception.getMessage()));
            return;
        } else if (exception instanceof InsufficientAuthenticationException) {
            setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED,
                    new KylinException(USER_UNAUTHORIZED));
            return;
        } else if (exception instanceof DisabledException) {
            setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED,
                    new KylinException(LOGIN_FAILED, MsgPicker.getMsg().getDISABLED_USER()));
            return;
        }
        boolean present = Optional.ofNullable(exception).map(Throwable::getCause)
                .filter(CommunicationException.class::isInstance).map(Throwable::getCause)
                .filter(javax.naming.CommunicationException.class::isInstance).map(Throwable::getCause)
                .filter(ConnectException.class::isInstance).isPresent();

        if (present) {
            setErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    new KylinException(USER_DATA_SOURCE_CONNECTION_FAILED,
                            MsgPicker.getMsg().getLDAP_USER_DATA_SOURCE_CONNECTION_FAILED()));
            return;
        }

        present = Optional.ofNullable(exception).map(Throwable::getCause)
                .filter(org.springframework.ldap.AuthenticationException.class::isInstance).map(Throwable::getCause)
                .filter(javax.naming.AuthenticationException.class::isInstance).isPresent();

        if (present) {
            setErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, new KylinException(
                    USER_DATA_SOURCE_CONNECTION_FAILED, MsgPicker.getMsg().getLDAP_USER_DATA_SOURCE_CONFIG_ERROR()));
            return;
        }

        setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED,
                new KylinException(USER_LOGIN_FAILED));
    }

    public void setErrorResponse(HttpServletRequest request, HttpServletResponse response, int statusCode, Exception ex)
            throws IOException {
        response.setStatus(statusCode);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        ErrorResponse errorResponse = new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(request), ex);
        String errorStr = JsonUtil.writeValueAsIndentString(errorResponse);
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.print(errorStr);
        writer.flush();
        writer.close();
    }

}
