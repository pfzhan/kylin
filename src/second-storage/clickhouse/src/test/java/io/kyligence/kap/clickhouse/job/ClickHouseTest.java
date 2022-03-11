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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.guava20.shaded.common.base.Strings;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClickHouseTest {

    @Before
    public void setUp() throws Exception {
        initNodeHelper();
    }

    public void initNodeHelper() {
        ClusterInfo cluster = new ClusterInfo();
        Map<String, List<Node>> clusterNodes = new HashMap<>();
        cluster.setCluster(clusterNodes);
        clusterNodes.put("pair1", Collections.singletonList(new Node().setName("node01").setIp("127.0.0.1").setPort(9000).setUser("default").setPassword("123456")));
        clusterNodes.put("pair2", Collections.singletonList(new Node().setName("node02").setIp("127.0.0.1").setPort(9000).setUser("default")));
        clusterNodes.put("pair3", Collections.singletonList(new Node().setName("node03").setIp("127.0.0.1").setPort(9000)));
        SecondStorageNodeHelper.initFromCluster(
                cluster,
                node -> ClickHouse.buildUrl(node.getIp(), node.getPort(), ClickHouseStorage.getJdbcUrlProperties(cluster, node)),
                nodes -> {
                    if (nodes.isEmpty()) {
                        return "";
                    }

                    StringBuilder sb = new StringBuilder();
                    for (Node node : nodes) {
                        if (Strings.isNullOrEmpty(sb.toString())) {
                            sb.append(node.getIp()).append(":").append(node.getPort());
                        } else {
                            sb.append(",").append(node.getIp()).append(":").append(node.getPort());
                        }
                    }
                    return ClickHouse.buildUrl(sb.toString(), ClickHouseStorage.getJdbcUrlProperties(cluster, nodes.get(0)));
                });
    }

    @Test
    public void createClickHouse() throws SQLException {
        ClickHouse clickHouse1 = new ClickHouse(SecondStorageNodeHelper.resolve("node01"));
        Assert.assertEquals("127.0.0.1:9000", clickHouse1.getShardName());
        ClickHouse clickHouse2 = new ClickHouse(SecondStorageNodeHelper.resolve("node02"));
        Assert.assertEquals("127.0.0.1:9000", clickHouse2.getShardName());
        ClickHouse clickHouse3 = new ClickHouse(SecondStorageNodeHelper.resolve("node03"));
        Assert.assertEquals("127.0.0.1:9000", clickHouse3.getShardName());
    }

    @Test
    public void extractParam() {
        val param = ClickHouse.extractParam(SecondStorageNodeHelper.resolve("node01"));
        Assert.assertEquals(2, param.size());
        val param2 = ClickHouse.extractParam(SecondStorageNodeHelper.resolve("node03"));
        Assert.assertEquals(0, param2.size());
    }

    @Test
    public void testJdbcUrlProperties() {
        Node node = new Node("node01", "127.0.0.1", 9000, "default", "123456");
        ClusterInfo cluster = new ClusterInfo();
        cluster.setKeepAliveTimeout("1000");
        cluster.setSocketTimeout("1000");
        Map<String, String> properties = ClickHouseStorage.getJdbcUrlProperties(cluster, node);
        Assert.assertEquals(properties.get(ClickHouse.KEEP_ALIVE_TIMEOUT), cluster.getKeepAliveTimeout());
        Assert.assertEquals(properties.get(ClickHouse.SOCKET_TIMEOUT), cluster.getSocketTimeout());
        Assert.assertEquals(properties.get(ClickHouse.USER), node.getUser());
        Assert.assertEquals(properties.get(ClickHouse.PASSWORD), node.getPassword());
    }
}