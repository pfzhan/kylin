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
package io.kyligence.kap.clickhouse.management;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;

public class ClickHouseConfigLoaderTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testTransformNode() throws IOException {
        ClusterInfo cluster = new ClusterInfo().setKeepAliveTimeout("600000").setSocketTimeout("600000");
        Map<String, List<Node>> clusterNode = new HashMap<>();
        clusterNode.put("pair0", Lists.newArrayList(new Node("node01", "127.0.0.1", 9000, "default", "", 111)));
        clusterNode.put("pair1", Lists.newArrayList(new Node("node02", "127.0.0.2", 9000, "default", "")));
        cluster.setCluster(clusterNode);
        cluster.transformNode();
        int size = cluster.getNodes().stream().filter(node -> {
            return node.getSSHPort() == 22;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, size);

        size = cluster.getNodes().stream().filter(node -> {
            return node.getSSHPort() == 111;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, size);

        File file = File.createTempFile("clickhouseTemp", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(
                JsonUtil.readValue(JsonUtil.writeValueAsString(cluster), Map.class),
                new PrintWriter(file, Charset.defaultCharset().name()));
        overwriteSystemProp(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());

        ClusterInfo clusterReader = ClickHouseConfigLoader.getInstance().getCluster();
        List<Node> allNodes = clusterReader.getNodes();
        int portSize = allNodes.stream().filter(node -> {
            return node.getSSHPort() == 22;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, portSize);

        portSize = allNodes.stream().filter(node -> {
            return node.getSSHPort() == 111;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, portSize);
    }
}