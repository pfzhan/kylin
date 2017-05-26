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
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

@Component(value = "kapUnauthorisedEntryPoint")
public class KapUnauthorisedEntryPoint implements AuthenticationEntryPoint {

    @Autowired
    private KapAuthenticationManager kapAuthenticationManager;

    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException exception)
            throws IOException, ServletException {
        Message msg = MsgPicker.getMsg();
        Throwable cause = exception;
        while (cause != null) {
            if (cause.getClass().getPackage().getName().startsWith("org.apache.hadoop.hbase")) {
                setErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        new InternalErrorException(msg.getHBASE_FAIL_WITHOUT_DETAIL()));
                return;
            }
            cause = cause.getCause();
        }

        String userName = null;
        if (exception instanceof LockedException)
            userName = exception.getCause().getMessage();

        if (kapAuthenticationManager.isUserLocked(userName)) {
            setErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST, exception);
            return;
        }

        setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED, exception);
    }

    public void setErrorResponse(HttpServletRequest request, HttpServletResponse response, int statusCode, Exception ex)
            throws IOException {
        response.setStatus(statusCode);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        ErrorResponse errorResponse = new ErrorResponse(request.getRequestURL().toString(), ex);
        String errorStr = JsonUtil.writeValueAsIndentString(errorResponse);
        ServletOutputStream out = response.getOutputStream();
        out.print(errorStr);
        out.flush();
        out.close();
    }

}
