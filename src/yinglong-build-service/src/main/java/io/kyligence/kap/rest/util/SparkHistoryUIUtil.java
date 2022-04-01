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

package io.kyligence.kap.rest.util;

import java.io.IOException;
import java.net.InetAddress;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.SneakyThrows;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.util.SparkUIUtil;
import org.apache.spark.deploy.history.HistoryServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component("SparkHistoryUIUtil")
@ConditionalOnProperty(name = "kylin.history-server.enable", havingValue = "true")
public class SparkHistoryUIUtil {

    @Autowired
    @Qualifier("historyServer")
    private HistoryServer historyServer;

    public static final String HISTORY_UI_BASE = "/history_server";

    public static final String KYLIN_HISTORY_UI_BASE = "/kylin" + HISTORY_UI_BASE;

    public static final String PROXY_LOCATION_BASE = KylinConfig.getInstanceFromEnv().getUIProxyLocation()
            + HISTORY_UI_BASE;

    public void proxy(HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws IOException {
        final String historyServerUrl = getHistoryServerUrl();
        String uriPath = servletRequest.getRequestURI().substring(KYLIN_HISTORY_UI_BASE.length());

        SparkUIUtil.resendSparkUIRequest(servletRequest, servletResponse, historyServerUrl, uriPath,
                PROXY_LOCATION_BASE);
    }

    @SneakyThrows
    private String getHistoryServerUrl() {
        return String.format("http://%s:%s", InetAddress.getLocalHost().getHostAddress(), historyServer.boundPort());
    }

    public static String getHistoryTrackerUrl(String appId) {
        return PROXY_LOCATION_BASE + "/history/" + appId;
    }

}
