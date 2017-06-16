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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.config.KafkaConsumerProperties;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectKafkaStats {

    private static final Logger logger = LoggerFactory.getLogger(CollectKafkaStats.class);
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    private static final String IP_PATTERN = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";
    private static final int SAMPLE_MSG_COUNT = 10;
    private static final int POLL_MESSAGE_TIMEOUT = 20000;

    //List topics
    public static Map<String, List<String>> getTopics(KafkaConfig kafkaConfig) {

        Map<String, List<String>> topicsMap = new HashMap<>();
        String brokers = KafkaClient.getKafkaBrokers(kafkaConfig);
        for (String broker : brokers.split(",")) {
            Properties property = KafkaConsumerProperties.getInstanceFromEnv().extractKafkaConfigToProperties();
            Consumer consumer = KafkaClient.getKafkaConsumer(broker, "sample", property);
            logger.info(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG + ":{}",
                    property.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();

            String key = identifyClusterByBrokers(brokers);
            for (Map.Entry<String, List<PartitionInfo>> topic : topics.entrySet()) {
                if (!isUsefulTopic(topic.getKey()))
                    continue;
                if (topicsMap.get(key) == null) {
                    List<String> topicNames = new ArrayList<>();
                    topicNames.add(topic.getKey());
                    topicsMap.put(key, topicNames);
                } else {
                    if (!topicsMap.get(key).contains(topic.getKey()))
                        topicsMap.get(key).add(topic.getKey());
                }
            }
            consumer.close();
            break;
        }
        return topicsMap;
    }

    public static List<String> getMessages(KafkaConfig kafkaConfig) {

        logger.info("Start to get sample messages from Kafka.");
        String topic = kafkaConfig.getTopic();
        String brokers = KafkaClient.getKafkaBrokers(kafkaConfig);
        List<String> samples = new ArrayList<>();
        for (String broker : brokers.split(",")) {
            logger.info("Trying to get messages from broker: {}", broker);
            Consumer consumer;
            ConsumerRecords<String, String> records;
            Properties property = KafkaConsumerProperties.getInstanceFromEnv().extractKafkaConfigToProperties();
            consumer = KafkaClient.getKafkaConsumer(broker, "sample", property);

            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

            if (partitionInfos.size() <= 0) {
                logger.warn("There are no partitions in topic: " + topic);
                break;
            }

            for (PartitionInfo partitionInfo : partitionInfos) {
                int partition = partitionInfo.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partition);

                consumer.assign(Arrays.asList(topicPartition));
                consumer.seekToEnd(Arrays.asList(topicPartition));
                long offset = consumer.position(topicPartition);
                if (offset <= 0) {
                    continue;
                } else if (offset < SAMPLE_MSG_COUNT)
                    consumer.seek(topicPartition, 0);
                else
                    consumer.seek(topicPartition, offset - SAMPLE_MSG_COUNT);

                logger.info("Ready to poll messages. Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
                records = consumer.poll(POLL_MESSAGE_TIMEOUT);
                if (records.isEmpty())
                    continue;
                else {
                    for (ConsumerRecord<String, String> record : records) {
                        if (samples.size() >= SAMPLE_MSG_COUNT)
                            break;
                        samples.add(record.value());
                    }
                    break;
                }
            }
            consumer.close();
            if (samples.size() == 0)
                continue;
        }
        logger.info("Get sample message size is: {}", samples.size());
        return samples;
    }

    private static boolean isUsefulTopic(String topic) {
        final Pattern UUId_PATTERN = Pattern.compile(UUID_PATTERN);
        if (UUId_PATTERN.matcher(topic).matches()) {
            return false;
        }

        if ("__consumer_offsets".equals(topic)) {
            return false;
        }
        return true;
    }

    private static String identifyClusterByBrokers(String brokers) {
        StringBuffer clusterName = new StringBuffer();
        for (String broker : brokers.split(",")) {
            String host = broker.substring(0, broker.lastIndexOf(":"));
            if (!StringUtils.isEmpty(host) && isIp(host))
                host = host.replace('.', '-');
            clusterName.append(host);
            clusterName.append(",");
        }
        String ret = clusterName.toString();
        if (ret.isEmpty())
            return ret;
        else
            return ret.substring(0, ret.length() - 1);
    }

    private static boolean isIp(String ipAddress) {
        Pattern pattern = Pattern.compile(IP_PATTERN);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.find();
    }
}
