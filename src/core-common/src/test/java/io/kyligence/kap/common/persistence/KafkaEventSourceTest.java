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
package io.kyligence.kap.common.persistence;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;

import io.kyligence.kap.common.persistence.event.EndUnit;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.StartUnit;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.mq.MessageQueue;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaEventSourceTest extends NLocalFileMetadataTestCase {

    @ClassRule
    public static SharedKafkaTestResource kafkaTestResource = new SharedKafkaTestResource();

    @Before
    public void init() {
        createTestMetadata();
        val connectString = kafkaTestResource.getKafkaConnectString();
        System.setProperty("kylin.metadata.mq-url",
                "kylin_metadata_" + UUID.randomUUID().toString() + "@kafka,bootstrap.servers='" + connectString + "'");
    }

    @After
    public void destroy() {
        System.clearProperty("kylin.metadata.mq-url");
    }

    @Test
    public void testSyncEvents() {
        Serializer<StringEntity> serializer = new JsonSerializer<>(StringEntity.class);

        val unit = "p0";
        for (int i = 0; i < 3; i++) {
            int finalI = i;
            UnitOfWork.doInTransactionWithRetry(() -> {
                val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                for (int j = 0; j < 1024; j++) {
                    val entity = new StringEntity("hello");
                    entity.setMvcc(finalI - 1);
                    store.checkAndPutResource("/" + unit + "/abc" + j, entity, serializer);
                }
                return 0;
            }, unit);
        }

        val eventStore = MessageQueue.getInstance(getTestConfig());
        AtomicInteger count = new AtomicInteger();
        eventStore.syncEvents(um -> {
            Assert.assertTrue(um.getMessages().get(0) instanceof StartUnit);
            Assert.assertEquals(1024 + 2, um.getMessages().size());
            count.getAndIncrement();
        });
        Assert.assertEquals(3, count.get());
    }

    @Test
    public void testStartConsumerFromPartialTransaction() throws Exception {
        Serializer<StringEntity> serializer = new JsonSerializer<>(StringEntity.class);
        val unit = "p0";

        final int INITIAL_COUNT = 5;
        for (int i = 0; i < INITIAL_COUNT; i++) {
            int finalI = i;
            UnitOfWork.doInTransactionWithRetry(() -> {
                val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                for (int j = 0; j < 1024; j++) {
                    val entity = new StringEntity("hello");
                    entity.setMvcc(finalI - 1);
                    store.checkAndPutResource("/" + unit + "/abc" + j, entity, serializer);
                }
                return 0;
            }, unit);
        }

        val eventStore = MessageQueue.getInstance(getTestConfig());
        val start = new StartUnit(UUID.randomUUID().toString());
        start.setKey(unit);
        val partialEvents = Lists.<Event> newArrayList(start);
        for (int i = 0; i < 800; i++) {
            val raw = new RawResource("/" + unit + "/abc" + i, ByteStreams.asByteSource("world".getBytes()), 0L, -1);
            val event = new ResourceCreateOrUpdateEvent(raw);
            event.setKey(unit);
            partialEvents.add(event);
        }
        eventStore.getEventPublisher().publish(new UnitMessages(partialEvents));

        AtomicInteger count = new AtomicInteger();
        eventStore.syncEvents(um -> count.getAndIncrement());
        Assert.assertEquals(INITIAL_COUNT, count.get());

        val otherEvents = Lists.<Event> newArrayList();
        for (int i = 800; i < 1024; i++) {
            val raw = new RawResource("/" + unit + "/abc" + i, ByteStreams.asByteSource("world".getBytes()), 0L, 3);
            val event = new ResourceCreateOrUpdateEvent(raw);
            event.setKey(unit);
            otherEvents.add(event);
        }
        val end = new EndUnit(start.getUnitId());
        end.setKey(unit);
        otherEvents.add(end);
        eventStore.getEventPublisher().publish(new UnitMessages(otherEvents));
        eventStore.startConsumer(um -> {
            Assert.assertTrue(um.getMessages().get(0) instanceof StartUnit);
            Assert.assertEquals(1024 + 2, um.getMessages().size());
            count.getAndIncrement();
        });
        val startTime = System.currentTimeMillis();
        while (count.get() == INITIAL_COUNT && (System.currentTimeMillis() - startTime < 5000)) {
            Thread.sleep(300);
        }

        Assert.assertEquals(INITIAL_COUNT + 1, count.get());
        eventStore.close();
    }

    @Test
    @Ignore("for develop")
    public void testKafkaTransaction() throws Exception {
        Serializer<StringEntity> serializer = new JsonSerializer<>(StringEntity.class);

        List<Thread> ts = Lists.newArrayList();
        for (int i = 0; i < 4; i++) {
            int finalI = i;
            val unit = "project" + (finalI % 4);
            val t = new Thread(() -> {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    for (int j = 0; j < 10; j++) {
                        val entity = new StringEntity("hello");
                        store.checkAndPutResource("/" + unit + "/abc" + j, entity, serializer);
                    }
                    return 0;
                }, unit);
            });
            t.start();
            ts.add(t);
        }
        for (Thread t : ts) {
            t.join();
        }
    }

    @Test
    @Ignore("for develop")
    public void testIsolation() throws Exception {
        val store = MessageQueue.getInstance(getTestConfig());
        val publisher = store.getEventPublisher();
        int counter = 0;
        while (true) {
            val events = Lists.<Event> newArrayList();
            counter = 0;
            for (int i = 0; i < 30; i++) {
                counter++;
                val event1 = new ResourceCreateOrUpdateEvent(new RawResource("/p1/123-" + counter,
                        ByteStreams.asByteSource(("abc-" + counter).getBytes()), 0L, -1L));
                event1.setKey("p1");
                events.add(event1);
            }
            publisher.publish(new UnitMessages(events));
            Thread.sleep(1000);
        }
    }

    @Test
    @Ignore
    public void testSubscriber() throws Exception {
        val store = MessageQueue.getInstance(getTestConfig());
        store.syncEvents(event -> {
            try {
                log.info("data {}", JsonUtil.writeValueAsString(event));
            } catch (JsonProcessingException ignore) {
            }
        });
        log.info("end sync");
        Thread.sleep(10000);
        store.startConsumer(event -> {
            try {
                log.info("data {}", JsonUtil.writeValueAsString(event));
            } catch (JsonProcessingException ignore) {
            }
        });

        while (true) {
            Thread.sleep(1000);
        }
    }
}
