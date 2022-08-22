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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.util;

import java.io.IOException;
import java.net.UnknownHostException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.service.JobInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;

@Component("JobDriverUIUtil")
public class JobDriverUIUtil {

    public static final String DRIVER_UI_BASE = "/driver_ui";

    public static final String KYLIN_DRIVER_UI_BASE = "/kylin" + DRIVER_UI_BASE;

    public static final String PROXY_LOCATION_BASE = KylinConfig.getInstanceFromEnv().getUIProxyLocation()
            + DRIVER_UI_BASE;

    @Autowired
    private JobInfoService jobInfoService;

    public void proxy(String project, String jobStep, HttpServletRequest servletRequest,
            HttpServletResponse servletResponse) throws IOException {
        final String originUiUrl = getOriginUiUrl(project, jobStep);
        if (StringUtils.isEmpty(originUiUrl)) {
            servletResponse.getWriter().write("track url not generated yet");
            return;
        }
        String requestBasePath = KYLIN_DRIVER_UI_BASE + "/" + project + "/" + jobStep;

        String uriPath = servletRequest.getRequestURI().substring(requestBasePath.length());
        try {
            SparkUIUtil.resendSparkUIRequest(servletRequest, servletResponse, originUiUrl, uriPath,
                    PROXY_LOCATION_BASE + "/" + project + "/" + jobStep);
        } catch (UnknownHostException e) {
            servletResponse.getWriter().write("track url invalid already ");
        }
    }

    @SneakyThrows
    private String getOriginUiUrl(String project, String jobStep) {
        String trackUrl = jobInfoService.getOriginTrackUrlByProjectAndStepId(project, jobStep);
        return trackUrl;
    }

    public static String getProxyUrl(String project, String jobStep) {
        return PROXY_LOCATION_BASE + "/" + project + "/" + jobStep;

    }

}
