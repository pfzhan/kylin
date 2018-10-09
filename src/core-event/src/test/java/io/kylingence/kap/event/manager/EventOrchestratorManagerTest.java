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
package io.kylingence.kap.event.manager;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class EventOrchestratorManagerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testShutdown() throws Exception {
        EventOrchestratorManager.destroyInstance();
        EventOrchestratorManager eventOrchestratorManager = EventOrchestratorManager.getInstance(getTestConfig());

        Field x = EventOrchestratorManager.class.getDeclaredField("INSTANCE_MAP");
        x.setAccessible(true);
        Map<String, EventOrchestrator> map = (Map<String, EventOrchestrator>) x.get(null);
        EventOrchestrator eventOrchestrator = map.get("@global");

        Field fetcherPoolField = EventOrchestrator.class.getDeclaredField("fetcherPool");
        fetcherPoolField.setAccessible(true);
        ExecutorService es1 = (ExecutorService) fetcherPoolField.get(eventOrchestrator);

        Field eventProcessPoolField = EventOrchestrator.class.getDeclaredField("eventProcessPool");
        eventProcessPoolField.setAccessible(true);
        ExecutorService es2 = (ExecutorService) eventProcessPoolField.get(eventOrchestrator);

        Assert.assertFalse(es1.isShutdown());
        Assert.assertFalse(es2.isShutdown());

        EventOrchestratorManager.destroyInstance();

        Assert.assertTrue(es1.isShutdown());
        Assert.assertTrue(es2.isShutdown());
    }

}
