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
package io.kyligence.kap.secondstorage.config;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.EncryptUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterTest {
    @Test
    public void testPassword() {
        String password = "123456";
        ClusterInfo cluster = new ClusterInfo();
        cluster.setPassword(password);
        Assert.assertEquals(password, cluster.getPassword());

        cluster.setPassword(EncryptUtil.encryptWithPrefix(password));
        Assert.assertEquals(password, cluster.getPassword());

        Node node = new Node();
        node.setPassword(password);
        Assert.assertEquals(password, node.getPassword());

        node = new Node("node01", "127.0.0.1", 9000, "default", "123456", 222);
        Assert.assertEquals("node01", node.getName());
        Assert.assertEquals(222, node.getSSHPort());
        node.setSSHPort(22);
        Assert.assertEquals(22, node.getSSHPort());

        node.setPassword(EncryptUtil.encryptWithPrefix(password));
        Assert.assertEquals(password, node.getPassword());
    }

    @Test
    public void testTransformNode() {
        ClusterInfo cluster = new ClusterInfo();
        Map<String, List<Node>> clusterNode = new HashMap<>();
        clusterNode.put("pair0", Lists.newArrayList(new Node("node01", "127.0.0.1", 9000, "default", "123456", 222)));
        clusterNode.put("pair1", Lists.newArrayList(new Node("node01", "127.0.0.1", 9000, "default", "123456")));
        cluster.setCluster(clusterNode);
        cluster.transformNode();
        int size = cluster.getNodes().stream().filter(node -> {
            return node.getSSHPort() == 22;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, size);
    }
}
