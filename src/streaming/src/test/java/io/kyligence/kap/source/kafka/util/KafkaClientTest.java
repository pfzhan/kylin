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
package io.kyligence.kap.source.kafka.util;

import io.kyligence.kap.streaming.util.ReflectionUtils;
import lombok.val;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class KafkaClientTest {

    @Test
    public void testConstructDefaultKafkaConsumerProperties() {
        val prop = KafkaClient.constructDefaultKafkaConsumerProperties("localhost:9092", "client1", new Properties());
        Assert.assertNotNull(prop.isEmpty());
        Assert.assertEquals("localhost:9092", prop.getProperty("bootstrap.servers"));
        Assert.assertEquals("client1", prop.getProperty("group.id"));
        Assert.assertEquals("false", prop.getProperty("enable.auto.commit"));
    }

    @Test
    public void testGetKafkaConsumer() {
        val prop = KafkaClient.constructDefaultKafkaConsumerProperties("localhost:9092", "client1", new Properties());
        val consumer = KafkaClient.getKafkaConsumer("localhost:9092", "client2");

        ConsumerCoordinator coordinator = (ConsumerCoordinator) ReflectionUtils.getField(consumer, "coordinator");
        String groupId = (String) ReflectionUtils.getField(coordinator, "groupId");
        Assert.assertEquals("client2", groupId);

        Metadata meta = (Metadata) ReflectionUtils.getField(consumer, "metadata");
        Cluster cluster = (Cluster) ReflectionUtils.getField(meta, "cluster");
        List<Node> nodes = cluster.nodes();
        Assert.assertEquals("localhost", nodes.get(0).host());
        Assert.assertEquals(9092, nodes.get(0).port());
    }
}
