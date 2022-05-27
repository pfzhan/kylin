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

package io.kyligence.kap.rest.filter;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.BOOLEAN_TYPE_CHECK;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

@Component
@Order
@Slf4j
public class SegmentsRequestFilter implements Filter {
    public static final Pattern REQUEST_URI_PATTERN = Pattern.compile("(/kylin/api/models/)\\S+(/segments)");
    public static final String BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME = "build_all_sub_partitions";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        if (request != null && httpServletRequest.getMethod().equals(HttpMethod.POST)
                && REQUEST_URI_PATTERN.matcher(httpServletRequest.getRequestURI()).matches()) {
            try {
                JsonNode bodyNode = JsonUtil
                        .readValueAsTree(IOUtils.toString(httpServletRequest.getInputStream(), StandardCharsets.UTF_8));
                Set<String> bodyKeys = Sets.newHashSet(bodyNode.fieldNames());
                if (bodyKeys.contains(BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME)) {
                    checkBooleanArg(BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME,
                            bodyNode.get(BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME).asText());
                }
            } catch (KylinException e) {
                MsgPicker.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
                ErrorCode.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));

                ErrorResponse errorResponse = new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(httpServletRequest),
                        e);
                byte[] responseBody = JsonUtil.writeValueAsBytes(errorResponse);

                HttpServletResponse httpServletResponse = (HttpServletResponse) response;
                httpServletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                httpServletResponse.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON.toString());
                httpServletResponse.getOutputStream().write(responseBody);
                return;
            }
        }

        chain.doFilter(request, response);
    }

    private void checkRequiredArg(String fieldName, Object fieldValue) {
        if (fieldValue == null || StringUtils.isEmpty(String.valueOf(fieldValue))) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, fieldName);
        }
    }

    public void checkBooleanArg(String fieldName, Object fieldValue) {
        checkRequiredArg(fieldName, fieldValue);
        String booleanString = String.valueOf(fieldValue);
        if (!String.valueOf(true).equalsIgnoreCase(booleanString)
                && !String.valueOf(false).equalsIgnoreCase(booleanString)) {
            throw new KylinException(BOOLEAN_TYPE_CHECK, booleanString, "Boolean");
        }
    }

    @Override
    public void destroy() {
        // just override it
    }
}
