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
package io.kyligence.kap.common.persistence.transaction.kafka;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.kyligence.kap.common.persistence.transaction.mq.MessageQueue;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.EndUnit;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.transaction.mq.EventPublisher;
import io.kyligence.kap.common.persistence.transaction.mq.MQPublishFailureException;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaMessageQueue extends MessageQueue {

    private final StorageURL url;
    private final ExecutorService consumeExecutor;
    private Map<String, KafkaProducer<String, Event>> producers = Maps.newConcurrentMap();
    private final String topic;
    private final Map<String, String> kafkaPropeties;

    public KafkaMessageQueue(KylinConfig config) {
        url = config.getMetadataMQUrl();
        kafkaPropeties = url.getAllParameters();
        topic = url.getIdentifier();
        consumeExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(CONSUMER_THREAD_NAME));
    }

    public EventPublisher getEventPublisher() {
        return unit -> {
            val events = unit.getMessages();
            if (CollectionUtils.isEmpty(events)) {
                return;
            }
            val key = events.get(0).getKey();
            initProducer(key);
            val producer = producers.get(key);
            try {
                producer.beginTransaction();
                for (Event event : events) {
                    val record = new ProducerRecord<String, Event>(topic, event.getKey(), event);
                    producer.send(record);
                }
                producer.commitTransaction();
            } catch (Exception e) {
                producer.abortTransaction();
                throw new MQPublishFailureException("kafka transaction failed", e);
            }
        };
    }

    public void startConsumer(Consumer<UnitMessages> eventConsumer) {
        consumeExecutor.submit(() -> {
            val consumer = getKafkaConsumer();
            Map<String, UnitMessages> processingUnit = Maps.newHashMap();
            while (true) {
                val consumerRecords = consumer.poll(1000);
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                processRecords(consumerRecords, processingUnit, consumer, eventConsumer);
            }
        });
    }

    @Override
    public void syncEvents(Consumer<UnitMessages> eventConsumer) {
        val originName = Thread.currentThread().getName();
        Thread.currentThread().setName(CONSUMER_THREAD_NAME);
        val consumer = getKafkaConsumer();
        val topicPartitions = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).collect(Collectors.toList());
        val endOffsets = consumer.endOffsets(topicPartitions);
        Map<String, UnitMessages> processingUnit = Maps.newHashMap();
        while (true) {
            boolean catchup = topicPartitions.stream().map(tp -> consumer.position(tp) >= endOffsets.get(tp))
                    .reduce(true, (l, r) -> l && r);
            if (catchup) {
                break;
            }
            val consumerRecords = consumer.poll(2000);
            if (consumerRecords.isEmpty()) {
                continue;
            }
            processRecords(consumerRecords, processingUnit, consumer, eventConsumer);
        }
        consumer.close();
        Thread.currentThread().setName(originName);
    }

    private KafkaConsumer<String, Event> getKafkaConsumer() {
        val consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, topic + "-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.kyligence.kap.common.persistence.transaction.kafka.EventDeserializer");

        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ignored) {
            // ignore it
        }
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-" + topic + "-" + hostname);
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        kafkaPropeties.forEach(consumerConfig::put);
        val consumer = new KafkaConsumer<String, Event>(consumerConfig);
        val partitions = consumer.partitionsFor(topic);
        val topicPartitions = partitions.stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                .collect(Collectors.toList());
        consumer.assign(topicPartitions);
        for (PartitionInfo partition : partitions) {
            val maybeOffset = eventStoreProperties.get("offset" + partition.partition());
            val topicPartition = new TopicPartition(topic, partition.partition());
            if (maybeOffset != null) {
                int offset = Integer.parseInt(maybeOffset);
                consumer.seek(topicPartition, offset);
            } else {
                consumer.seekToBeginning(Lists.newArrayList(topicPartition));
            }
        }
        return consumer;
    }

    private void processRecords(ConsumerRecords<String, Event> consumerRecords,
            Map<String, UnitMessages> processingUnit, KafkaConsumer<String, Event> consumer,
            Consumer<UnitMessages> eventConsumer) {
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = Maps.newHashMap();
        log.debug("records count {}", consumerRecords.count());
        for (ConsumerRecord<String, Event> record : consumerRecords) {
            val unit = processingUnit.computeIfAbsent(record.key(), key -> new UnitMessages());
            unit.getMessages().add(record.value());
            if (record.value() instanceof EndUnit) {
                eventConsumer.accept(unit);
                processingUnit.put(record.key(), new UnitMessages());
                offsetAndMetadataMap.put(new TopicPartition(topic, record.partition()),
                        new OffsetAndMetadata(record.offset()));
            }
        }
        consumer.commitSync(offsetAndMetadataMap);
        offsetAndMetadataMap
                .forEach((p, offset) -> eventStoreProperties.put("offset" + p.partition(), (offset.offset() + 1) + ""));
        log.debug("offset map is {}, {}", eventStoreProperties);
    }

    private synchronized void initProducer(String key) {
        if (producers.get(key) != null) {
            return;
        }
        val producerConfig = new Properties();
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, topic + "-" + key + "-producer");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kylin-" + topic + "-" + key);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.kyligence.kap.common.persistence.transaction.kafka.EventSerializer");
        producerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024); // 10M
        producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        url.getAllParameters().forEach(producerConfig::put);
        val producer = new KafkaProducer<String, Event>(producerConfig);
        producer.initTransactions();
        producers.put(key, producer);
    }

    @Override
    public void close() throws IOException {
        for (KafkaProducer<String, Event> producer : producers.values()) {
            producer.close();
        }
        ExecutorServiceUtil.shutdownGracefully(consumeExecutor, 10);
    }
}
