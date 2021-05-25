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

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class KafkaConfigTest {
    @Test
    public void testCreate() {
        val kafkaConfig1 = new KafkaConfig();

        ReflectionUtils.setField(kafkaConfig1, "name", "config_name");
        ReflectionUtils.setField(kafkaConfig1, "kafkaBootstrapServers", "localhost:9092");

        val kafkaConfig2 = new KafkaConfig(kafkaConfig1);

        String name = kafkaConfig2.getName();
        String bootstrapServers = kafkaConfig2.getKafkaBootstrapServers();

        Assert.assertEquals("config_name", name);
        Assert.assertEquals("localhost:9092", bootstrapServers);
    }

    @Test
    public void testGetKafkaParam() {
        val kafkaConfig1 = new KafkaConfig();

        ReflectionUtils.setField(kafkaConfig1, "database", "DEFAULT");
        ReflectionUtils.setField(kafkaConfig1, "kafkaBootstrapServers", "localhost:9092");
        ReflectionUtils.setField(kafkaConfig1, "name", "config_name");
        ReflectionUtils.setField(kafkaConfig1, "subscribe", "subscribe_item");
        ReflectionUtils.setField(kafkaConfig1, "startingOffsets", "startingOffset_item");

        Map<String, String> params = kafkaConfig1.getKafkaParam();

        Assert.assertEquals("localhost:9092", params.get("kafka.bootstrap.servers"));
        Assert.assertEquals("subscribe_item", params.get("subscribe"));
        Assert.assertEquals("startingOffset_item", params.get("startingOffsets"));
        Assert.assertEquals("false", params.get("failOnDataLoss"));
    }
}
