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
package io.kyligence.kap.rest;


import static org.apache.kylin.rest.exception.ServerErrorCode.SYSTEM_IS_RECOVER;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerRule;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.Server;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.interceptor.ProjectInfoParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectBasedRoundRobinRule extends AbstractLoadBalancerRule {

    public ProjectBasedRoundRobinRule() {
    }

    public ProjectBasedRoundRobinRule(ILoadBalancer lb) {
        this();
        setLoadBalancer(lb);
    }

    @Override
    public Server choose(Object key) {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes())
                .getRequest();
        return choose(request);
    }

    private Server choose(HttpServletRequest request) {
        Pair<String, ServletRequest> projectInfo = ProjectInfoParser.parseProjectInfo(request);
        String project = projectInfo.getFirst();
        String owner = EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).getEpochOwner(project);
        if (StringUtils.isBlank(owner)) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(SYSTEM_IS_RECOVER, msg.getLEADERS_HANDLE_OVER());
        }
        String[] host = owner.split(":");
        Server server = new Server(host[0], Integer.valueOf(host[1]));
        log.info("Request {} is redirecting to {}.", request.getRequestURI(), server);
        return server;
    }

    @Override
    public void initWithNiwsConfig(IClientConfig iClientConfig) {

    }
}
