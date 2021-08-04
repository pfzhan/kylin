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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.rest.request.StreamingRequest;
import io.kyligence.kap.rest.service.StreamingTableService;
import io.kyligence.kap.rest.service.TableExtService;
import lombok.val;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

public class StreamingTableControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private StreamingTableService streamingTableService;

    @Mock
    private TableExtService tableExtService;

    @InjectMocks
    private StreamingTableController streamingTableController = Mockito.spy(new StreamingTableController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static String PROJECT = "streaming_test";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(streamingTableController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(streamingTableController, "streamingTableService", streamingTableService);
        ReflectionTestUtils.setField(streamingTableController, "tableExtService", tableExtService);

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
    public void testSaveStreamingTable() throws Exception {
        val request = new StreamingRequest();
        request.setProject(PROJECT);
        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setSubscribe("ssb_topic");
        request.setKafkaConfig(kafkaConfig);
        val tableDesc = new TableDesc();
        tableDesc.setName("SSB.KAFKA_STR");
        request.setTableDesc(tableDesc);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/streaming_tables/table")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(streamingTableController).saveStreamingTable(Mockito.any(StreamingRequest.class));
    }

    @Test
    public void testSaveStreamingTableColumnsMatch() throws Exception {
        String streamingTable = "DEFAULT.SSB_TOPIC";
        val request = new StreamingRequest();
        request.setProject(PROJECT);
        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setSubscribe("ssb_topic");
        kafkaConfig.setBatchTable("SSB.P_LINEORDER_STR");
        request.setKafkaConfig(kafkaConfig);
        val tableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).getTableDesc(streamingTable);
        request.setTableDesc(tableDesc);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/streaming_tables/table")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(streamingTableController).saveStreamingTable(Mockito.any(StreamingRequest.class));
    }

    @Test
    public void testUpdateStreamingTable() throws Exception {
        val request = new StreamingRequest();
        request.setProject(PROJECT);
        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setSubscribe("ssb_topic");
        request.setKafkaConfig(kafkaConfig);
        val tableDesc = new TableDesc();
        tableDesc.setName("SSB.KAFKA_STR");
        request.setTableDesc(tableDesc);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_tables/table")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(streamingTableController).updateStreamingTable(Mockito.any(StreamingRequest.class));
    }
}
