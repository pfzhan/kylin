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
package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.rest.request.StreamingRequest;
import io.kyligence.kap.rest.service.KafkaService;
import lombok.val;

public class KafkaControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private KafkaService kafkaService = Mockito.spy(KafkaService.class);

    @InjectMocks
    private KafkaController kafkaController = Mockito.spy(new KafkaController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static String PROJECT = "streaming_test";
    private static final String PARSER_NAME = "io.kyligence.kap.parser.JsonDataParser1";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(kafkaController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(kafkaController, "kafkaService", kafkaService);
    }

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetTopics() throws Exception {
        val request = mockStreamingRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).getTopics(Mockito.any(StreamingRequest.class));
    }

    @Test
    public void testGetMessages() throws Exception {
        val request = mockStreamingRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/messages").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).getMessages(Mockito.any(StreamingRequest.class));
    }

    @Test
    public void testGetMessages1() throws Exception {
        val request = mockStreamingRequest();
        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setKafkaBootstrapServers("127.0.0.1:9092");
        kafkaConfig.setSubscribe("ssb_topic");
        kafkaConfig.setStartingOffsets("latest");
        request.setKafkaConfig(kafkaConfig);
        val messages = Arrays.asList(ByteBuffer.allocate(10));
        Mockito.when(
                kafkaService.getMessages(request.getKafkaConfig(), request.getProject(), request.getClusterIndex()))
                .thenReturn(messages);
        Mockito.when(kafkaService.getMessageTypeAndDecodedMessages(messages)).thenReturn(new HashMap<String, Object>());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/messages").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).getMessages(request);
    }

    @Test
    public void testConvertMessageToFlatMap() throws Exception {
        val request = mockStreamingRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/convert").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).convertMessageToFlatMap(Mockito.any(StreamingRequest.class));
    }

    private StreamingRequest mockStreamingRequest() {
        val request = new StreamingRequest();
        request.setProject(PROJECT);
        request.setKafkaConfig(Mockito.mock(KafkaConfig.class));
        return request;
    }

    @Test
    public void testGetParser() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/kafka/parsers").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).getParser(Mockito.anyString());
    }

    @Test
    public void testRemoveParser() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/kafka/parser").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("class_name", PARSER_NAME)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).removeParser(Mockito.anyString(), Mockito.anyString());
    }

}
