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

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class KafkaServiceTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    /*   Test Configs:
    
        kafkaConfig: "{"name":"test1","topic":"kylindemo","timeout":"60000","bufferSize":"65536","parserName":"org.apache.kylin.source.kafka.TimedJsonStreamParser","margin":"300000","clusters":[{"brokers":[{"id":"1","host":"sandbox","port":"9092"}]}],"parserProperties":"","timestampField":"order_time"}"
        project : "KAP_Sample"
        streamingConfig : "{"name":"test1","type":"kafka"}"
        tableData : "{"name":"test1","source_type":1,"columns":[{"id":1,"name":"amount","datatype":"decimal"},{"id":2,"name":"category","datatype":"varchar(256)"},{"id":3,"name":"order_time","datatype":"timestamp"},{"id":4,"name":"device","datatype":"varchar(256)"},{"id":5,"name":"qty","datatype":"int"},{"id":6,"name":"user_id","datatype":"varchar(256)"},{"id":7,"name":"user_age","datatype":"int"},{"id":8,"name":"user_gender","datatype":"varchar(256)"},{"id":9,"name":"currency","datatype":"varchar(256)"},{"id":10,"name":"country","datatype":"varchar(256)"},{"id":11,"name":"year_start","datatype":"date"},{"id":12,"name":"quarter_start","datatype":"date"},{"id":13,"name":"month_start","datatype":"date"},{"id":14,"name":"week_start","datatype":"date"},{"id":15,"name":"day_start","datatype":"date"},{"id":16,"name":"hour_start","datatype":"timestamp"},{"id":17,"name":"minute_start","datatype":"timestamp"}],"database":"DEFAULT"}"
    
        Test Data:
    
        0: "{"amount":5.179352160855055,"category":"ELECTRONIC","order_time":1510654252378,"device":"Windows","qty":8,"user":{"id":"b29d053f-f13a-42de-b6a1-f86570bca2bf","age":11,"gender":"Female"},"currency":"USD","country":"CANADA"}"
        1: "{"amount":9.246631999471688,"category":"ELECTRONIC","order_time":1510654252388,"device":"Windows","qty":9,"user":{"id":"f6626c61-87fe-44b8-9e8f-862325463d96","age":22,"gender":"Female"},"currency":"USD","country":"CHINA"}"
        2: "{"amount":22.884003649631325,"category":"Other","order_time":1510654252398,"device":"Andriod","qty":9,"user":{"id":"5031f27e-7fb6-4077-8af7-6cf3cb1f249b","age":18,"gender":"Male"},"currency":"USD","country":"AUSTRALIA"}"
    */
    @Test
    public void testConvertMessagesToSamples() throws IOException {
        String kafkaConfigObject = "{\"name\":\"test1\",\"topic\":\"kylindemo\",\"timeout\":\"60000\","
                + "\"bufferSize\":\"65536\",\"parserName\":\"org.apache.kylin.source.kafka.TimedJsonStreamParser\","
                + "\"margin\":\"300000\",\"clusters\":[{\"brokers\":[{\"id\":\"1\",\"host\":\"sandbox\","
                + "\"port\":\"9092\"}]}],\"parserProperties\":\"\",\"timestampField\":\"order_time\"}";
        String project = "KAP_Sample";
        String tableData = "{\"name\":\"test1\",\"source_type\":1,\"columns\":[{\"id\":1,\"name\":\"amount\","
                + "\"datatype\":\"decimal\"},{\"id\":2,\"name\":\"category\",\"datatype\":\"varchar(256)\"},{\"id\":3,"
                + "\"name\":\"order_time\",\"datatype\":\"timestamp\"},{\"id\":4,\"name\":\"device\",\"datatype\":\"varchar(256)\"},"
                + "{\"id\":5,\"name\":\"qty\",\"datatype\":\"int\"},{\"id\":6,\"name\":\"user_id\",\"datatype\":\"varchar(256)\"},"
                + "{\"id\":7,\"name\":\"user_age\",\"datatype\":\"int\"},{\"id\":8,\"name\":\"user_gender\",\"datatype\":\"varchar(256)\"},"
                + "{\"id\":9,\"name\":\"currency\",\"datatype\":\"varchar(256)\"},{\"id\":10,\"name\":\"country\",\"datatype\":\"varchar(256)\"},"
                + "{\"id\":11,\"name\":\"year_start\",\"datatype\":\"date\"},{\"id\":12,\"name\":\"quarter_start\","
                + "\"datatype\":\"date\"},{\"id\":13,\"name\":\"month_start\",\"datatype\":\"date\"},{\"id\":14,\"name\":\"week_start\","
                + "\"datatype\":\"date\"},{\"id\":15,\"name\":\"day_start\",\"datatype\":\"date\"},{\"id\":16,\"name\":\"hour_start\","
                + "\"datatype\":\"timestamp\"},{\"id\":17,\"name\":\"minute_start\",\"datatype\":\"timestamp\"}],"
                + "\"database\":\"DEFAULT\"}";
        List<String> messages = new ArrayList<>();
        messages.add("{\"amount\":5.179352160855055,\"category\":\"ELECTRONIC\","
                + "\"order_time\":1510654252378,\"device\":\"Windows\",\"qty\":8,"
                + "\"user\":{\"id\":\"b29d053f-f13a-42de-b6a1-f86570bca2bf\",\"age\":11,"
                + "\"gender\":\"Female\"},\"currency\":\"USD\",\"country\":\"CANADA\"}");
        messages.add("{\"amount\":9.246631999471688,\"category\":\"ELECTRONIC\","
                + "\"order_time\":1510654252388,\"device\":\"Windows\",\"qty\":9,"
                + "\"user\":{\"id\":\"f6626c61-87fe-44b8-9e8f-862325463d96\",\"age\":22,"
                + "\"gender\":\"Female\"},\"currency\":\"USD\",\"country\":\"CHINA\"}");
        messages.add("{\"amount\":22.884003649631325,\"category\":\"Other\","
                + "\"order_time\":1510654252398,\"device\":\"Andriod\",\"qty\":9,"
                + "\"user\":{\"id\":\"5031f27e-7fb6-4077-8af7-6cf3cb1f249b\",\"age\":18,"
                + "\"gender\":\"Male\"},\"currency\":\"USD\",\"country\":\"AUSTRALIA\"}");
        KafkaService kafkaService = new KafkaService();
        TableDesc tableDesc = null;

        tableDesc = JsonUtil.readValue(tableData, TableDesc.class);
        tableDesc.setUuid(UUID.randomUUID().toString());
        kafkaService.getTableManager().saveSourceTable(tableDesc, project);

        KafkaConfig kafkaConfig = JsonUtil.readValue(kafkaConfigObject, KafkaConfig.class);
        kafkaConfig.setName(tableDesc.getIdentity());
        kafkaConfig.setUuid(UUID.randomUUID().toString());
        kafkaService.getKafkaManager().saveKafkaConfig(kafkaConfig);
        kafkaService.getKafkaManager().reloadKafkaConfigLocal(tableDesc.getIdentity());
        Assert.assertTrue(kafkaService.getTableManager().getTableDesc(tableDesc.getIdentity(), project).getUuid()
                .equals(tableDesc.getUuid()));
        Assert.assertTrue(kafkaService.getKafkaManager().getKafkaConfig(kafkaConfig.getName()).getUuid()
                .equals(kafkaConfig.getUuid()));

        List<String[]> result = kafkaService.convertMessagesToSamples(tableDesc.getIdentity(), project, messages);
        Assert.assertTrue(result.size() == 17);
        Assert.assertTrue(result.get(0).length == 3);
        Assert.assertTrue("22.884003649631325".equals(result.get(0)[2]));
    }
}
