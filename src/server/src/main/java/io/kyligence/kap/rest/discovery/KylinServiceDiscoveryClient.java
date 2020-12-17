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
package io.kyligence.kap.rest.discovery;

import static io.kyligence.kap.common.util.ClusterConstant.ServerModeEnum;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperDiscoveryClient;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnZookeeperEnabled
@Component
@Slf4j
public class KylinServiceDiscoveryClient implements KylinServiceDiscovery {

    private final ZookeeperDiscoveryClient discoveryClient;

    public KylinServiceDiscoveryClient(ZookeeperDiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
    }

    public String getLocalServiceServer() {
        val instance = discoveryClient.getLocalServiceInstance();
        return instance.getHost() + ":" + instance.getPort();
    }

    @Override
    public List<ServerInfoResponse> getServerInfoByServerMode(@Nullable ServerModeEnum... serverModeEnums) {
        List<ServerInfoResponse> serverInfoRespLists = new ArrayList<>();

        if (ArrayUtils.isEmpty(serverModeEnums)) {
            return serverInfoRespLists;
        }

        for (val nodeType : serverModeEnums) {
            for (val host : getServerStrByServerMode(nodeType)) {
                serverInfoRespLists.add(new ServerInfoResponse(host, nodeType.getName()));
            }
        }

        return serverInfoRespLists;
    }

    private List<String> getServerStrByServerMode(@Nonnull ServerModeEnum serverModeEnum) {
        Preconditions.checkNotNull(serverModeEnum, "server mode is null");

        String serverModeName = serverModeEnum.getName();
        val list = discoveryClient.getInstances(serverModeName);
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }

        return list.stream().map(serviceInstance -> serviceInstance.getHost() + ":" + serviceInstance.getPort())
                .collect(Collectors.toList());
    }

}
