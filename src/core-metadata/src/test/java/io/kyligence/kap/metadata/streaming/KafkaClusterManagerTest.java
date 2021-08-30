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
package io.kyligence.kap.metadata.streaming;

import java.util.LinkedList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class KafkaClusterManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetHistoryKafkaCluster() {
        val mgr = KafkaClusterManager.newInstance(getTestConfig());
        val histKafkaCluster = mgr.getKafkaCluster();
        Assert.assertNull(histKafkaCluster);

        val brokers = "127.0.0.1:9092";
        val kafkaCluster = new KafkaCluster();
        LinkedList<String> lists = Lists.newLinkedList();
        lists.addFirst(brokers);
        kafkaCluster.setHistoryKafkaBrokers(lists);
        mgr.updateKafkaBroker(kafkaCluster);
        val histKafkaCluster1 = mgr.getKafkaCluster();
        Assert.assertNotNull(histKafkaCluster1);
        Assert.assertEquals(brokers, histKafkaCluster1.getHistoryKafkaBrokers().get(0));
        Assert.assertEquals(KafkaCluster.RESOURCE_NAME, histKafkaCluster1.resourceName());
        Assert.assertEquals(KafkaCluster.RESOURCE_NAME, histKafkaCluster1.getIdentity());
    }

    @Test
    public void testUpdateKafkaBroker() {
        val mgr = KafkaClusterManager.getInstance(getTestConfig());
        val kafkaCluster = new KafkaCluster();
        LinkedList<String> lists = Lists.newLinkedList();
        lists.addFirst("192.169.1.100:9092");
        kafkaCluster.setHistoryKafkaBrokers(lists);
        val result = mgr.updateKafkaBroker(kafkaCluster);
        Assert.assertEquals(result.getHistoryKafkaBrokers(), kafkaCluster.getHistoryKafkaBrokers());

        thrown.expect(IllegalArgumentException.class);
        mgr.updateKafkaBroker(null);
    }

}
