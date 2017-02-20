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
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaClusterConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectKafkaStats {

    private static final Logger logger = LoggerFactory.getLogger(CollectKafkaStats.class);
    final static String uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    final static String ipPattern = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";
    final static long FETCH_MSG_TIMEOUT = 20000;
    final static int LIST_TOPIC_TIMEOUT = 12000;
    final static int MSG_AMOUNT = 10;

    public static Map<String, List<String>> getTopics(KafkaConfig kafkaConfig) {
        Map<String, List<String>> topicsMap = new HashMap<>();
        Map<String, List<String>> clustersMap = getCluster(kafkaConfig);

        for (Map.Entry<String, List<String>> cluster : clustersMap.entrySet()) {
            for (String broker : cluster.getValue()) {
                Properties property = new Properties();
                //this property must greater than 10000
                property.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, LIST_TOPIC_TIMEOUT);
                Consumer consumer = KafkaClient.getKafkaConsumer(broker, "sample", property);
                logger.info("Trying to get messages from broker: " + broker);
                if (null == consumer)
                    throw new IllegalArgumentException("The given cluster: " + broker + "is not found");

                Map<String, List<PartitionInfo>> topics = consumer.listTopics();
                if (0 == topics.size())
                    logger.info("There are no topics in the given broker: " + broker + ", please check if the broker is reasonable!");
                for (Map.Entry<String, List<PartitionInfo>> topic : topics.entrySet()) {
                    if (!isUsefulTopic(topic.getKey()))
                        continue;
                    if (topicsMap.get(cluster.getKey()) == null) {
                        List<String> topicNames = new ArrayList<>();
                        topicNames.add(topic.getKey());
                        topicsMap.put(cluster.getKey(), topicNames);
                    } else {
                        if (!topicsMap.get(cluster.getKey()).contains(topic.getKey()))
                            topicsMap.get(cluster.getKey()).add(topic.getKey());
                    }
                }
                consumer.close();
            }
        }
        return topicsMap;
    }

    public static Map<String, List<String>> getBrokers(KafkaConfig kafkaConfig) {
        Map<String, List<String>> topicBrokerMap = new HashMap<>();
        Map<String, List<String>> clustersMap = getCluster(kafkaConfig);

        for (Map.Entry<String, List<String>> cluster : clustersMap.entrySet()) {
            for (String broker : cluster.getValue()) {
                Consumer consumer = KafkaClient.getKafkaConsumer(broker, "test", null);
                if (null == consumer)
                    throw new IllegalArgumentException("The given cluster: " + broker + "is not found");

                Map<String, List<PartitionInfo>> topics = consumer.listTopics();
                for (Map.Entry<String, List<PartitionInfo>> topic : topics.entrySet()) {
                    if (!isUsefulTopic(topic.getKey()))
                        continue;
                    if (topicBrokerMap.get(topic.getKey()) == null) {
                        List<String> brokerList = new ArrayList<>();
                        brokerList.add(broker);
                        topicBrokerMap.put(topic.getKey(), brokerList);
                    } else {
                        if (!topicBrokerMap.get(topic.getKey()).contains(broker))
                            topicBrokerMap.get(topic.getKey()).add(broker);
                    }
                }
            }
        }
        return topicBrokerMap;
    }

    public static List<String> getMessageByTopic(String cluster, String topic, KafkaConfig kafkaConfig) {
        Map<String, List<String>> clustersMap = getCluster(kafkaConfig);
        Map<String, List<String>> topicsMap = getBrokers(kafkaConfig);

        List<String> availableBrokers = new ArrayList<>();
        List<String> brokersByTopic = topicsMap.get(topic);
        if (null == brokersByTopic)
            throw new IllegalArgumentException("There are no available brokers for the given topic: " + topic);

        List<String> brokersByCluster = clustersMap.get(cluster);

        for (String broker : brokersByTopic) {
            if (brokersByCluster.contains(broker))
                availableBrokers.add(broker);
        }

        return collectSamples(topic, availableBrokers);
    }

    public static List<String> getMessageByTopic(KafkaConfig kafkaConfig) {
        Map<String, List<String>> topicsMap = getBrokers(kafkaConfig);

        String topic = kafkaConfig.getTopic();
        List<String> brokersByTopic = topicsMap.get(topic);
        if (null == brokersByTopic)
            throw new IllegalArgumentException("There are no available brokers for the given topic: " + topic);

        return collectSamples(topic, brokersByTopic);
    }

    private static List<String> collectSamples(String topic, List<String> brokersByTopic) {
        List<String> samples = new ArrayList<>();

        Consumer consumer = null;
        ConsumerRecords<String, String> records = null;
        for (String broker : brokersByTopic) {
            consumer = KafkaClient.getKafkaConsumer(broker, "sample", null);

            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            int id = partitionInfos.get(0).partition();
            consumer.assign(Arrays.asList(new TopicPartition(topic, id)));
            consumer.seekToEnd(Arrays.asList(new TopicPartition(topic, id)));
            long pos = consumer.position(new TopicPartition(topic, id));
            consumer.seek(new TopicPartition(topic, id), pos - MSG_AMOUNT);

            records = consumer.poll(FETCH_MSG_TIMEOUT);
            if (records.isEmpty())
                throw new IllegalStateException("There are no available messages in this topic: " + topic);
            else {
                for (ConsumerRecord<String, String> record : records) {
                    samples.add(record.value());
                }
            }
        }
        consumer.close();
        return samples;
    }

    private static Map<String, List<String>> getCluster(KafkaConfig kafkaConfig) {
        Map<String, List<String>> clustersMap = new HashMap<>();
        for (KafkaClusterConfig kafkaClusterConfig : kafkaConfig.getKafkaClusterConfigs()) {
            String clusterName = IdentifyClusterByBrokers(kafkaClusterConfig.getBrokerConfigs());
            for (BrokerConfig brokerConfig : kafkaClusterConfig.getBrokerConfigs()) {
                String brokerUrl = brokerConfig.getHost() + ":" + brokerConfig.getPort();
                if (null == clustersMap.get(clusterName)) {
                    List<String> brokerList = new ArrayList<>();
                    brokerList.add(brokerUrl);
                    clustersMap.put(clusterName, brokerList);
                } else {
                    if (!clustersMap.get(clusterName).contains(brokerUrl))
                        clustersMap.get(clusterName).add(brokerUrl);
                }
            }
        }
        return clustersMap;
    }

    private static boolean isUsefulTopic(String topic) {
        final Pattern UUId_PATTERN = Pattern.compile(uuidPattern);
        if (UUId_PATTERN.matcher(topic).matches()) {
            return false;
        }

        if ("__consumer_offsets".equals(topic)) {
            return false;
        }
        return true;
    }

    private static String IdentifyClusterByBrokers(List<BrokerConfig> brokerConfigList) {
        StringBuffer clusterName = new StringBuffer();
        for (BrokerConfig brokerConfig : brokerConfigList) {
            String host = brokerConfig.getHost();
            if (!StringUtils.isEmpty(host) && isIp(host))
                host = host.replace('.', '-');
            clusterName.append(host);
            clusterName.append("|");
        }
        String ret = clusterName.toString();
        if (ret.isEmpty())
            return ret;
        else
            return ret.substring(0, ret.length() - 1);
    }

    private static boolean isIp(String ipAddress) {
        Pattern pattern = Pattern.compile(ipPattern);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.find();
    }
}
