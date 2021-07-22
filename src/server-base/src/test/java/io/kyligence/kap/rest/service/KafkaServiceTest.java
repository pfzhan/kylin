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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.metadata.streaming.ReflectionUtils;
import lombok.val;
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
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.Map;


public class KafkaServiceTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private final KafkaService kafkaService = Mockito.spy(KafkaService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private static final String brokerServer = "localhost:19093";
    private static final String PROJECT = "streaming_test";

    KafkaConfig kafkaConfig = new KafkaConfig();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(kafkaService, "aclEvaluate", aclEvaluate);
        init();
    }

    public void init() {
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
    public void testCheckBrokerStatus() {

        try {
            kafkaService.checkBrokerStatus(kafkaConfig);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            KylinException kylinException = (KylinException) e;
            ErrorResponse errorResponse = new ErrorResponse("http://localhost:7070/kylin/api/kafka/topics", kylinException);
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

}
