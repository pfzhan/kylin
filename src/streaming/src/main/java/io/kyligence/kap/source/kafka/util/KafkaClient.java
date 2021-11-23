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

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.streaming.jobs.StreamingJobUtils;
import org.apache.kylin.common.util.Pair;

public class KafkaClient {
    private static Consumer<Object, Object> mockup;
    /**
     * load jaas file's text into kafkaJaasTextPair to avoid multiple read file
     */
    private static final Pair<Boolean, String> kafkaJaasTextPair = new Pair<>(false, null);

    private KafkaClient() {
        throw new IllegalStateException("Utility class");
    }

    public static Consumer<Object, Object> getKafkaConsumer(String brokers, String consumerGroup,
            Properties properties) {
        Properties props = constructDefaultKafkaConsumerProperties(brokers, consumerGroup, properties);
        if (mockup != null) {
            return mockup;
        } else {
            return new KafkaConsumer<>(props);
        }
    }

    public static Consumer<Object, Object> getKafkaConsumer(String brokers, String consumerGroup) {
        return getKafkaConsumer(brokers, consumerGroup, new Properties());
    }

    public static AdminClient getKafkaAdminClient(String brokers, String consumerGroup) {
        return getKafkaAdminClient(brokers, consumerGroup, new Properties());
    }

    public static AdminClient getKafkaAdminClient(String brokers, String consumerGroup, Properties properties) {
        Properties props = constructDefaultKafkaAdminClientProperties(brokers, consumerGroup, properties);
        return AdminClient.create(props);
    }

    public static Properties constructDefaultKafkaAdminClientProperties(String brokers, String consumerGroup,
            Properties properties) {
        Properties props = new Properties();
        setSaslJaasConf(props);
        props.put("bootstrap.servers", brokers);
        props.put("group.id", consumerGroup);
        if (properties != null) {
            props.putAll(properties);
        }
        return props;
    }

    public static Properties constructDefaultKafkaConsumerProperties(String brokers, String consumerGroup,
            Properties properties) {
        Properties props = new Properties();
        setSaslJaasConf(props);
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

    private static void setSaslJaasConf(Properties props) {
        props.putAll(KylinConfig.getInstanceFromEnv().getStreamingKafkaConfigOverride());
        synchronized (kafkaJaasTextPair) {
            if (!kafkaJaasTextPair.getFirst().booleanValue()) {
                kafkaJaasTextPair.setSecond(StreamingJobUtils.extractKafkaSaslJaasConf());
                kafkaJaasTextPair.setFirst(true);
            }
        }
        if (StringUtils.isNotEmpty(kafkaJaasTextPair.getSecond())) {
            props.put(SASL_JAAS_CONFIG, kafkaJaasTextPair.getSecond());
        }
    }
}
