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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.persistence.transaction.mq.EventStore;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaEventSourceTest extends NLocalFileMetadataTestCase {

    @Before
    public void init() {
        createTestMetadata();
        System.setProperty("kylin.event-store.url", "kylin_metadata@kafka,bootstrap.servers=sandbox:9093");
    }

    @Test
    @Ignore("for develop")
    public void testKafkaTransaction() throws Exception {
        val store = EventStore.getInstance(getTestConfig());
        val publisher = store.getEventPublisher();
        publisher.publish(Lists.newArrayList(new ResourceCreateOrUpdateEvent(null), new ResourceDeleteEvent("/path")));

        store.startConsumer(event -> {
            log.info("receive {} {}", event, event.getClass());
        });
        while (true) {
            Thread.sleep(10000);
        }
    }
}
