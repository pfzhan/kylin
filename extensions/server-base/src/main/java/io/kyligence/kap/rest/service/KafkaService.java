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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaClusterConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.springframework.stereotype.Component;

@Component("kafkaClusterService")
public class KafkaService extends BasicService {

    final String uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    final long timeOut = 10000;
    final int msgCount = 10;

    public Map<String, List<String>> getTopics(KafkaConfig kafkaConfig) {
        Map<String, List<String>> topicsMap = new HashMap<>();
        Map<String, List<String>> clustersMap = getCluster(kafkaConfig);

        for (Map.Entry<String, List<String>> cluster : clustersMap.entrySet()) {
            for (String broker : cluster.getValue()) {
                Consumer consumer = KafkaClient.getKafkaConsumer(broker, null, null);
                if (null == consumer)
                    throw new IllegalArgumentException("The given cluster: " + broker + "is not found");

                Map<String, List<PartitionInfo>> topics = consumer.listTopics();
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
            }
        }
        return topicsMap;
    }

    public Map<String, List<String>> getBrokers(KafkaConfig kafkaConfig) {
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
                            topicBrokerMap.get(cluster.getKey()).add(topic.getKey());
                    }
                }
            }
        }
        return topicBrokerMap;
    }

    public List<String> getMessageByTopic(String cluster, String topic, KafkaConfig kafkaConfig) {
        Map<String, List<String>> clustersMap = getCluster(kafkaConfig);
        Map<String, List<String>> topicsMap = getBrokers(kafkaConfig);

        List<String> msgList = new ArrayList<>();
        List<String> availableBrokers = new ArrayList<>();
        List<String> brokersByTopic = topicsMap.get(topic);
        if (null == brokersByTopic)
            throw new IllegalArgumentException("There are no available brokers for the given topic: " + topic);

        List<String> brokersByCluster = clustersMap.get(cluster);

        for (String broker : brokersByTopic) {
            if (brokersByCluster.contains(broker))
                availableBrokers.add(broker);
        }

        ConsumerRecords<String, String> records = null;
        for (String broker : availableBrokers) {
            Consumer consumer = KafkaClient.getKafkaConsumer(broker, "test", null);
            consumer.subscribe(Arrays.asList(topic));
            records = consumer.poll(timeOut);
            if (records.isEmpty())
                continue;
            else
                break;
        }

        int count = 0;
        for (ConsumerRecord<String, String> record : records) {
            msgList.add(record.value());
            if (count++ < msgCount)
                break;
        }
        return msgList;
    }

    private Map<String, List<String>> getCluster(KafkaConfig kafkaConfig) {
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
                        clustersMap.get(clusterName).add(kafkaConfig.getName());
                }
            }
        }
        return clustersMap;
    }

    private boolean isUsefulTopic(String topic) {
        final Pattern UUId_PATTERN = Pattern.compile(uuidPattern);
        if (UUId_PATTERN.matcher(topic).matches()) {
            return false;
        }

        if ("__consumer_offsets".equals(topic)) {
            return false;
        }
        return true;
    }

    /*
    public List<KafkaConfig> listKafkaConfigsByProject(String project) throws IOException {
        List<KafkaConfig> kafkaConfigList = new ArrayList<>();
        List<TableDesc> tables = ProjectManager.getInstance(getConfig()).listDefinedTables(project);
        for (TableDesc tableDesc : tables) {
            for (Map.Entry<String, KafkaConfig> kafka : kafkaMap.getMap().entrySet()) {
                tableDesc.getIdentity().equalsIgnoreCase(kafka.getKey());
                kafkaConfigList.add(kafka.getValue());
            }
        }
        return kafkaConfigList;
    }
    
    public Map<String, List<String>> getClustersByProject(String project) {
        Map<String, List<String>> clusterMap = new HashMap<>();
        for (KafkaConfig kafkaConfig : listAllKafkaConfigs()) {
            for (KafkaClusterConfig clusterConfig : kafkaConfig.getKafkaClusterConfigs()) {
                String clusterName = IdentifyClusterByBrokers(clusterConfig.getBrokerConfigs());
                if (null == clusterMap.get(clusterName)) {
                    List<String> tableList = new ArrayList<>();
                    tableList.add(kafkaConfig.getName());
                    clusterMap.put(clusterName, tableList);
                } else {
                    clusterMap.get(clusterName).add(kafkaConfig.getName());
                }
            }
        }
        return clusterMap;
    }
    */
    private String IdentifyClusterByBrokers(List<BrokerConfig> brokerConfigList) {
        StringBuffer clusterName = new StringBuffer();
        for (BrokerConfig brokerConfig : brokerConfigList) {
            clusterName.append(brokerConfig.getHost());
            clusterName.append("|");
        }
        return clusterName.toString();
    }
}
