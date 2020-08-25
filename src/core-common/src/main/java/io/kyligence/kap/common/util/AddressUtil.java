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
package io.kyligence.kap.common.util;

import io.kyligence.kap.common.obf.IKeep;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
public class AddressUtil implements IKeep {
    public static String getLocalInstance() {
        String serverIp = "127.0.0.1";
        try {
            serverIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.warn("use the InetAddress get local ip failed!", e);
        }
        return serverIp + ":" + KylinConfig.getInstanceFromEnv().getServerPort();
    }

    /**
     * refer org.springframework.cloud.zookeeper.discovery.ZookeeperDiscoveryProperties#getInstanceHost()
     * @return
     */
    public static String getZkLocalInstance() {
        String hostName = "localhost";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("use the InetAddress get local ip failed!", e);
        }
        return hostName + ":" + KylinConfig.getInstanceFromEnv().getServerPort();
    }

    public static String convertHost(String serverHost) {
        String hostArr;
        val hostAndPort = serverHost.split(":");
        String host = hostAndPort[0];
        String port = hostAndPort[1];
        try {
            hostArr = InetAddress.getByName(host).getHostAddress() + ":" + port;
        } catch (UnknownHostException e) {
            hostArr = "127.0.0.1:" + port;
        }
        return hostArr;
    }

    public static String getMockPortAddress() {
        return getLocalInstance().split(":")[0] + ":" + "0000";
    }

    public static String getLocalServerInfo() {
        String hostName = "localhost";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        }catch (UnknownHostException e){
            log.warn("use the InetAddress get host name failed!", e);
        }
        String host = hostName + "_" + KylinConfig.getInstanceFromEnv().getServerPort();
        return host.replaceAll("[^(_a-zA-Z0-9)]", "");
    }
}
