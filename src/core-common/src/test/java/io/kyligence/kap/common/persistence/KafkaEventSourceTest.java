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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.mq.EventStore;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaEventSourceTest extends NLocalFileMetadataTestCase {

    @Before
    public void init() {
        createTestMetadata();
        System.setProperty("kylin.metadata.url", "kylin_metadata@hdfs,bootstrap.servers=sandbox:9092");
        System.setProperty("kylin.metadata.mq-type", "kafka");
    }

    @Test
    @Ignore("for develop")
    public void testKafkaTransaction() throws Exception {
        val raw = new RawResource("/abc", ByteStreams.asByteSource("123".getBytes()), 1L, 0L);

        val executors = Executors.newFixedThreadPool(4);
        List<Future> futures = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            val future = executors.submit(() -> {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    store.checkAndPutResource("/abc", ByteStreams.asByteSource("abc".getBytes()), -1);
                    store.checkAndPutResource("/abc2", ByteStreams.asByteSource("abc".getBytes()), -1);
                    store.checkAndPutResource("/abc3", ByteStreams.asByteSource("abc".getBytes()), -1);
                    store.deleteResource("/abc");
                    return 0;
                }, "project" + (finalI % 4));
                //                val publisher = store.getEventPublisher();
                //                val events = Lists.newArrayList(new ResourceCreateOrUpdateEvent(raw), new ResourceDeleteEvent("/path"));
                //                for (ResourceRelatedEvent event : events) {
                //                    event.setKey("project" + (finalI % 4));
                //                }
                //                publisher.publish(events);
            });
            futures.add(future);
        }
        while (true) {
            Thread.sleep(1000);
        }
    }

    @Test
    @Ignore
    public void testSubscriber() throws Exception {
        val store = EventStore.getInstance(getTestConfig());
        store.syncEvents(event -> {
            try {
                log.info("data {}", JsonUtil.writeValueAsIndentString(event));
            } catch (JsonProcessingException ignore) {
            }
        });
    }
}
