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

package io.kyligence.kap.rest.service;

import static io.kyligence.kap.source.kafka.CollectKafkaStats.DEFAULT_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.source.kafka.CollectKafkaStats;
import lombok.val;

public class KafkaServiceTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private final KafkaService kafkaService = Mockito.spy(KafkaService.class);

    @Mock
    private final CustomFileService customFileService = Mockito.spy(CustomFileService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private static final String brokerServer = "localhost:19093";
    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static final String PARSER_NAME = "io.kyligence.kap.parser.JsonDataParser1";
    private static String JAR_ABS_PATH;

    KafkaConfig kafkaConfig = new KafkaConfig();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(kafkaService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(customFileService, "aclEvaluate", aclEvaluate);
        init();
    }

    public void init() {
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setName("P_LINEORDER");
        kafkaConfig.setProject("streaming_test");
        kafkaConfig.setKafkaBootstrapServers(brokerServer);
        kafkaConfig.setSubscribe("ssb-topic1");
        kafkaConfig.setStartingOffsets("latest");
        kafkaConfig.setBatchTable("");
        kafkaConfig.setParserName(DEFAULT_PARSER);
    }

    public void initJar() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // ../examples/test_data/21767/metadata
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toString();
    }

    @Test
    public void testCheckBrokerStatus() {
        try {
            kafkaService.checkBrokerStatus(kafkaConfig);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            KylinException kylinException = (KylinException) e;
            ErrorResponse errorResponse = new ErrorResponse("http://localhost:7070/kylin/api/kafka/topics",
                    kylinException);
            Assert.assertEquals("http://localhost:7070/kylin/api/kafka/topics", errorResponse.url);
            val dataMap = (Map<String, Object>) errorResponse.getData();
            Assert.assertEquals(1, dataMap.size());
            Assert.assertEquals(Collections.singletonList(brokerServer), dataMap.get("failed_servers"));
        }
    }

    @Test
    public void testGetTopics() {
        expectedException.expect(KylinException.class);
        expectedException.expectMessage(Message.getInstance().getBROKER_TIMEOUT_MESSAGE());
        kafkaService.getTopics(kafkaConfig, PROJECT, "test");
    }

    @Test
    public void testGetMessage() {
        expectedException.expect(KylinException.class);
        expectedException.expectMessage("Can’t get sample data. Please check and try again.");
        kafkaService.getMessages(kafkaConfig, PROJECT, 1);
    }

    @Test
    public void testGetMessageTypeAndDecodedMessages() {
        val value = ByteBuffer.allocate(10);
        value.put("msg-1".getBytes());
        value.flip();
        val messages = Arrays.asList(value);
        val decoded = kafkaService.getMessageTypeAndDecodedMessages(messages);
        val decodedMessages = (List) decoded.get("message");
        Assert.assertEquals(1, decodedMessages.size());
    }

    @Test
    public void testConvertSampleMessageToFlatMap() {
        val result = kafkaService.convertSampleMessageToFlatMap("streaming_test", kafkaConfig,
                CollectKafkaStats.JSON_MESSAGE, "{\"a\": 2, \"b\": 2, \"timestamp\": \"2000-01-01 05:06:12\"}");
        Assert.assertEquals(3, result.size());
        Assert.assertThrows(CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED.getMsg(), KylinException.class,
                () -> kafkaService.convertSampleMessageToFlatMap("streaming_test", kafkaConfig,
                        CollectKafkaStats.JSON_MESSAGE,
                        "{\"_a\": 2, \"b\": 2, \"timestamp\": \"2000-01-01 05:06:12\"}"));
    }

    @Test
    public void testGetParsers() {
        List<String> parsers = kafkaService.getParsers(PROJECT);
        Assert.assertFalse(parsers.isEmpty());
    }

    @Test
    public void testRemoveParser() throws IOException {
        initJar();
        String jarType = "STREAMING_CUSTOM_PARSER";
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        String jarHdfsPath = customFileService.uploadCustomJar(jarFile, PROJECT, jarType);
        customFileService.loadParserJar(JAR_NAME, jarHdfsPath, PROJECT);
        kafkaService.removeParser(PROJECT, PARSER_NAME);
        customFileService.removeJar(PROJECT, JAR_NAME, jarType);
    }

}
