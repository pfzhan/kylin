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

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.msg.MsgPicker;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import io.kyligence.kap.metadata.project.NProjectLoader;
import io.kyligence.kap.rest.interceptor.RepeatableRequestBodyFilter.RepeatableBodyRequestWrapper;
import lombok.Data;
import lombok.val;

public class KEInterceptor extends HandlerInterceptorAdapter {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String lang = request.getHeader("Accept-Language");
        MsgPicker.setMsg(lang);

        String project = request.getParameter("project");
        if (StringUtils.isEmpty(project) && request instanceof RepeatableBodyRequestWrapper) {
            try {
                val projectRequest = JsonUtil.readValue(((RepeatableBodyRequestWrapper) request).getBody(),
                        ProjectRequest.class);
                if (projectRequest != null) {
                    project = projectRequest.getProject();
                }
            } catch (IOException ignored) {
                // ignore JSON exception
            }
        }
        NProjectLoader.updateCache(project);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
            throws Exception {
        NProjectLoader.removeCache();
        super.afterCompletion(request, response, handler, ex);
    }

    @Data
    public static class ProjectRequest {

        private String project;

    }
}
