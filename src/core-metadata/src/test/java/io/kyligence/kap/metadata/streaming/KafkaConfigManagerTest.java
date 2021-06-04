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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KafkaConfigManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private KafkaConfigManager mgr;
    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = KafkaConfigManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetKafkaConfig() {
        val emptyId = "";
        Assert.assertNull(mgr.getKafkaConfig(emptyId));

        val id = "DEFAULT.SSB_TOPIC";
        val kafkaConf = mgr.getKafkaConfig(id);
        Assert.assertNotNull(kafkaConf);

        val id_not_existed = "DEFAULT.SSB_TOPIC_not_existed";
        val kafkaConfNotExisted = mgr.getKafkaConfig(id_not_existed);
        Assert.assertNull(kafkaConfNotExisted);
    }

    @Test
    public void testCreateKafkaConfig() {
        val kafkaConf = new KafkaConfig();
        kafkaConf.setProject(PROJECT);
        kafkaConf.setDatabase("DEFAULT");
        kafkaConf.setName("TPCH_TOPIC");
        kafkaConf.setKafkaBootstrapServers("10.1.2.210:9094");
        kafkaConf.setStartingOffsets("earliest");
        mgr.createKafkaConfig(kafkaConf);
        val kafkaConfig = KafkaConfigManager.getInstance(getTestConfig(), PROJECT)
                .getKafkaConfig("DEFAULT.TPCH_TOPIC");
        Assert.assertEquals("10.1.2.210:9094", kafkaConfig.getKafkaBootstrapServers());
        Assert.assertEquals("earliest", kafkaConfig.getStartingOffsets());
    }

    @Test
    public void testUpdateKafkaConfig() {
        val kafkaConf = new KafkaConfig();
        kafkaConf.setDatabase("default");
        kafkaConf.setName("empty");
        try {
            mgr.updateKafkaConfig(kafkaConf);
        }catch (IllegalArgumentException e) {
            Assert.assertEquals("Kafka Config 'empty' does not exist.", e.getMessage());
        }
        val kafkaConfig = KafkaConfigManager.getInstance(getTestConfig(), PROJECT)
                .getKafkaConfig("DEFAULT.SSB_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9094");
        kafkaConfig.setStartingOffsets("earliest");
        mgr.updateKafkaConfig(kafkaConfig);
        Assert.assertEquals("10.1.2.210:9094", kafkaConfig.getKafkaBootstrapServers());
        Assert.assertEquals("earliest", kafkaConfig.getStartingOffsets());
    }

    @Test
    public void testRemoveKafkaConfig() {
        val id = "DEFAULT.SSB_TOPIC";
        val kafkaConf = mgr.getKafkaConfig(id);
        Assert.assertNotNull(kafkaConf);

        val id_not_existed = "DEFAULT.SSB_TOPIC_not_existed";
        mgr.removeKafkaConfig(id);
        val kafkaConf1 = mgr.getKafkaConfig(id);
        Assert.assertNull(kafkaConf1);

    }

    @Test
    public void testBatchTableAlias() {
        val id = "SSB.P_LINEORDER_STREAMING";
        val kafkaConf = mgr.getKafkaConfig(id);
        Assert.assertNotNull(kafkaConf);
        Assert.assertTrue(kafkaConf.hasBatchTable());
        Assert.assertEquals("LINEORDER_HIVE", kafkaConf.getBatchTableAlias());
    }
}
