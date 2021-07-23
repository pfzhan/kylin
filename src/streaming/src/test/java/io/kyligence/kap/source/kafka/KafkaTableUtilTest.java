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
package io.kyligence.kap.source.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.Message;
import org.apache.spark.utils.EmbededServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.streaming.util.ReflectionUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;

public class KafkaTableUtilTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";

    String brokerServer = "localhost:19093";

    EmbededServer server;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        server = new EmbededServer(19093);
        server.setup();
        init();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
        server.teardown();
    }

    KafkaConfig kafkaConfig = new KafkaConfig();

    public void init() {
        val topic = "ssb-topic";
        server.createTopic(topic, 3);

        val topic1 = "ssb-topic1";
        val topic2 = "ssb-topic2";
        val topic3 = "default-topic";
        server.createTopic(topic1, 1);
        server.createTopic(topic2, 1);
        server.createTopic(topic3, 1);

        val messages = new HashMap();
        messages.put("Message 1", 1);
        messages.put("Message 3", 2);
        messages.put("Message 4", 4);
        server.sendMessages(topic1, messages);
        server.flush();

        ReflectionUtils.setField(kafkaConfig, "database", "SSB");
        ReflectionUtils.setField(kafkaConfig, "name", "P_LINEORDER");
        ReflectionUtils.setField(kafkaConfig, "project", "streaming_test");
        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", brokerServer);
        ReflectionUtils.setField(kafkaConfig, "subscribe", "ssb-topic1");
        ReflectionUtils.setField(kafkaConfig, "startingOffsets", "latest");
        ReflectionUtils.setField(kafkaConfig, "batchTable", "");
        ReflectionUtils.setField(kafkaConfig, "parserName", "io.kyligence.kap.parser.TimedJsonStreamParser");
    }

    @Test
    public void testGetMessages() {
        val messages = KafkaTableUtil.getMessages(kafkaConfig, 1);
        Assert.assertEquals(7, messages.size());

        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", "");
        try {
            KafkaTableUtil.getMessages(kafkaConfig, 1);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_BROKER_DEFINITION.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", brokerServer);
    }

    @Test
    public void testGetMessageTypeAndDecodedMessages() {
        try {
            KafkaTableUtil.getMessageTypeAndDecodedMessages(null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
        }
        try {
            KafkaTableUtil.getMessageTypeAndDecodedMessages(new ArrayList(0));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
        }

        val messages = KafkaTableUtil.getMessages(kafkaConfig, 1);
        val decoded = KafkaTableUtil.getMessageTypeAndDecodedMessages(messages);

        val decodedMessages = (List) decoded.get("message");
        Assert.assertEquals(7, decodedMessages.size());

        val msg = "{\"timestamp\": \"2000-01-01 05:06:12\"}";
        val base64Msg = new String(Base64.encodeBase64(msg.getBytes()));
        val msgList = Arrays.asList(ByteBuffer.allocate(base64Msg.length()));
        val map = KafkaTableUtil.getMessageTypeAndDecodedMessages(msgList);
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(CollectKafkaStats.BINARY_MESSAGE, map.get("message_type"));
        Assert.assertEquals(1, ((List) map.get("message")).size());
    }

    @Test
    public void testValidateKafkaConfig() {
        Assert.assertTrue(KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers()));

        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", "");
        Assert.assertFalse(KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers()));
        Assert.assertFalse(KafkaTableUtil.validateKafkaConfig(null));
        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", brokerServer);
    }

    @Test
    public void testGetTopics() {
        String brokenBrokerServer = "1.1.1.1:9092,2.2.2.2:9092,3.3.3.3:9092";
        kafkaConfig.setKafkaBootstrapServers(brokenBrokerServer);
        val brokenBrokers1 = KafkaTableUtil.getBrokenBrokers(kafkaConfig);
        Assert.assertEquals(3, brokenBrokers1.size());

        val brokenBrokers = CollectKafkaStats.getBrokenBrokers(kafkaConfig);
        Assert.assertEquals(3, brokenBrokers.size());

        kafkaConfig.setKafkaBootstrapServers(brokerServer);
        val brokenBrokers2 = KafkaTableUtil.getBrokenBrokers(kafkaConfig);
        Assert.assertEquals(0, brokenBrokers2.size());

        val topics = KafkaTableUtil.getTopics(kafkaConfig, "ssb");
        Assert.assertEquals(3, topics.get("kafka-cluster-1").size());

        val topics2 = CollectKafkaStats.getTopics(kafkaConfig, "default");
        Assert.assertEquals(1, topics2.get("kafka-cluster-1").size());

        val messages = CollectKafkaStats.getMessages(kafkaConfig, 1);
        Assert.assertEquals(7, messages.size());

        try {
            ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", "");
            KafkaTableUtil.getTopics(kafkaConfig, "default");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }

        try {
            ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", "");
            KafkaTableUtil.getBrokenBrokers(kafkaConfig);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            KylinException kylinException = (KylinException) e;
            Assert.assertEquals(kylinException, kylinException.withData("test"));
            Assert.assertEquals("test", kylinException.getData());
        }

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getBROKER_TIMEOUT_MESSAGE());
        kafkaConfig.setKafkaBootstrapServers(brokenBrokerServer);
        CollectKafkaStats.getTopics(kafkaConfig, "ssb");

        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", brokerServer);
    }

    @Test
    public void testConvertMessageToFlatMap() {
        try {
            KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE, null);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE, "");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE, "test");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KafkaTableUtil.convertMessageToFlatMap(null, CollectKafkaStats.JSON_MESSAGE, "test");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        val result = KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE,
                "{\"a\": 2, \"b\": 2, \"timestamp\": \"2000-01-01 05:06:12\"}");

        Assert.assertEquals(3, result.size());
    }

    @Test
    public void testValidateStreamMessageType() {
        KafkaTableUtil.validateStreamMessageType(CollectKafkaStats.JSON_MESSAGE);
        KafkaTableUtil.validateStreamMessageType(CollectKafkaStats.BINARY_MESSAGE);
        try {
            KafkaTableUtil.validateStreamMessageType("");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            KafkaTableUtil.validateStreamMessageType("InvalidMessageType");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testDeserializeSampleMessage() {
        try {
            KafkaTableUtil.deserializeSampleMessage(CollectKafkaStats.JSON_MESSAGE, "a");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        val buff = KafkaTableUtil.deserializeSampleMessage(CollectKafkaStats.JSON_MESSAGE, "{}");
        Assert.assertEquals("{}", new String(buff.array()));

        val msg = "{\"timestamp\": \"2000-01-01 05:06:12\"}";
        val base64Msg = new String(Base64.encodeBase64(msg.getBytes()));
        val base64Buff = KafkaTableUtil.deserializeSampleMessage(CollectKafkaStats.BINARY_MESSAGE, base64Msg);

        Assert.assertEquals(msg, new String(base64Buff.array()));

        try {
            KafkaTableUtil.deserializeSampleMessage("error-type", "{}");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

}
