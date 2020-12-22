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
package io.kyligence.kap.common.persistence.transaction;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class MessageSynchronizationTest extends NLocalFileMetadataTestCase {

    private final Charset charset = Charset.defaultCharset();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void replayTest() {
        val synchronize = MessageSynchronization.getInstance(getTestConfig());
        val events = createEvents();
        synchronize.replayInTransaction(new UnitMessages(events));
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val raw = resourceStore.getResource("/default/abc.json");
        Assert.assertEquals(1, raw.getMvcc());
        val empty = resourceStore.getResource("/default/abc3.json");
        Assert.assertNull(empty);
    }

    private List<Event> createEvents() {
        val event1 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc.json", ByteStreams.asByteSource("version1".getBytes(charset)), 0L, 0));
        val event2 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc2.json", ByteStreams.asByteSource("abc2".getBytes(charset)), 0L, 0));
        val event3 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc.json", ByteStreams.asByteSource("version2".getBytes(charset)), 0L, 1));
        val event4 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc3.json", ByteStreams.asByteSource("42".getBytes(charset)), 0L, 0));
        val event5 = new ResourceDeleteEvent("/default/abc3.json");
        return Lists.newArrayList(event1, event2, event3, event4, event5).stream().peek(e -> e.setKey("default"))
                .collect(Collectors.toList());
    }

}
