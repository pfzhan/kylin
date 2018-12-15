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
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.transaction.mq.EventPublisher;
import io.kyligence.kap.common.persistence.transaction.mq.EventStore;
import io.kyligence.kap.common.persistence.transaction.mq.MQPublishFailureException;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaEventStore extends EventStore {

    private final StorageURL url;
    private KafkaProducer<String, Event> producer;
    private final String topic;
    private final Map<String, String> kafkaPropeties;

    public KafkaEventStore(KylinConfig config) {
        url = config.getMetadataUrl();
        kafkaPropeties = url.getAllParameters();
        topic = url.getIdentifier();
    }

    public EventPublisher getEventPublisher() {
        initProducer();
        return events -> {
            try {
                producer.beginTransaction();
                log.debug("events are {}", events);
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

    public void startConsumer(Consumer<Event> eventConsumer) {
        val consumerThread = new Thread(() -> {
            val consumer = getKafkaConsumer();
            consumer.subscribe(Lists.newArrayList(topic));
            while (true) {
                val consumerRecords = consumer.poll(1000);
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                consumerRecords.forEach(record -> {
                    eventConsumer.accept(record.value());
                });
                consumer.commitSync();
                updateOffset(consumer);
            }
        }, CONSUMER_THREAD_NAME);
        consumerThread.start();
    }

    @Override
    public void syncEvents(Consumer<Event> eventConsumer) {
        val originName = Thread.currentThread().getName();
        Thread.currentThread().setName(CONSUMER_THREAD_NAME);
        val consumer = getKafkaConsumer();
        consumer.assign(consumer.partitionsFor(topic).stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                .collect(Collectors.toList()));
        val partitions = consumer.partitionsFor(topic);
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
        int retry = 3;
        while (true) {
            val consumerRecords = consumer.poll(2000);
            if (consumerRecords.isEmpty()) {
                if (retry == 0) {
                    break;
                }
                retry--;
            }
            consumerRecords.forEach(record -> eventConsumer.accept(record.value()));
            consumer.commitSync();
            updateOffset(consumer);
        }
        consumer.close();
        Thread.currentThread().setName(originName);
    }

    private KafkaConsumer<String, Event> getKafkaConsumer() {
        val consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "transaction-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.kyligence.kap.common.persistence.transaction.kafka.EventDeserializer");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-group0");
        kafkaPropeties.forEach(consumerConfig::put);
        val consumer = new KafkaConsumer<String, Event>(consumerConfig);
        return consumer;
    }

    private void updateOffset(KafkaConsumer<String, Event> consumer) {
        val partitions = consumer.partitionsFor(topic);
        partitions.forEach(p -> {
            long pos = consumer.position(new TopicPartition(topic, p.partition()));
            eventStoreProperties.put("offset" + p.partition(), pos + "");
        });
    }

    private synchronized void initProducer() {
        if (producer != null) {
            return;
        }
        val producerConfig = new Properties();
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, topic + "-producer");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kylin-transactional-" + topic);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.kyligence.kap.common.persistence.transaction.kafka.EventSerializer");
        url.getAllParameters().forEach(producerConfig::put);
        producer = new KafkaProducer<>(producerConfig);
        producer.initTransactions();
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }
}
