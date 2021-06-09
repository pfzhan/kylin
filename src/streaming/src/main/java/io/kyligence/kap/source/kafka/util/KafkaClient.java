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

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaClient {

    private KafkaClient() {
        throw new IllegalStateException("Utility class");
    }

    public static KafkaConsumer getKafkaConsumer(String brokers, String consumerGroup, Properties properties) {
        Properties props = constructDefaultKafkaConsumerProperties(brokers, consumerGroup, properties);
        return new KafkaConsumer<>(props);
    }

    public static KafkaConsumer getKafkaConsumer(String brokers, String consumerGroup) {
        Properties props = new Properties();
        return getKafkaConsumer(brokers, consumerGroup, props);
    }

    public static Properties constructDefaultKafkaConsumerProperties(String brokers, String consumerGroup,
            Properties properties) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteBufferDeserializer.class.getName());
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", "false");
        if (properties != null) {
            props.putAll(properties);
        }
        return props;
    }

    public static int getPartitions(Map<String, String> kafkaParam) {
        final String brokers = kafkaParam.get("kafka.bootstrap.servers");
        final String topic = kafkaParam.get("subscribe");
        final Properties property = new Properties();
        try (final KafkaConsumer consumer = KafkaClient.getKafkaConsumer(brokers,
                "kylin_streaming_kafka_client_" + UUID.randomUUID(), property)) {
            return consumer.partitionsFor(topic).size();
        }
    }
}