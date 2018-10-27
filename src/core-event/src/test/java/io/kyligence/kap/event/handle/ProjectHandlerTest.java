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
package io.kyligence.kap.event.handle;

import static io.kyligence.kap.event.manager.EventManager.GLOBAL;

import java.lang.reflect.Field;
import java.util.Map;

import io.kyligence.kap.event.model.AddProjectEvent;
import io.kyligence.kap.event.model.EventContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.manager.EventOrchestrator;
import io.kyligence.kap.event.manager.EventOrchestratorManager;

public class ProjectHandlerTest extends NLocalFileMetadataTestCase {

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
    public void testHandlerIdempotent() throws Exception {
        getTestConfig().setProperty("kylin.server.mode", "query");
        getTestConfig().setProperty("kylin.event.wait-for-job-finished", "false");

        EventOrchestratorManager.destroyInstance();
        EventOrchestratorManager eventOrchestratorManager = EventOrchestratorManager.getInstance(getTestConfig());

        Class<?> clazz = eventOrchestratorManager.getClass();
        Field field = clazz.getDeclaredField("INSTANCE_MAP");
        field.setAccessible(true);
        Map<String, EventOrchestrator> instanceMap = (Map<String, EventOrchestrator>) field.get(eventOrchestratorManager);
        instanceMap.put(GLOBAL, new EventOrchestrator(GLOBAL, getTestConfig()));
        int instanceSize = instanceMap.size();

        AddProjectEvent event = new AddProjectEvent();
        event.setApproved(true);
        event.setProject(DEFAULT_PROJECT);
        EventContext eventContext = new EventContext(event, getTestConfig());
        ProjectHandler handler = new ProjectHandler();

        handler.handle(eventContext);

        instanceMap = (Map<String, EventOrchestrator>) field.get(eventOrchestratorManager);
        int instanceSize2 = instanceMap.size();

        Assert.assertEquals(instanceSize + 1, instanceSize2);

        // do handle again
        handler.handle(eventContext);

        instanceMap = (Map<String, EventOrchestrator>) field.get(eventOrchestratorManager);
        int instanceSize3 = instanceMap.size();
        Assert.assertEquals(instanceSize3, instanceSize2);

        getTestConfig().setProperty("kylin.server.mode", "all");

    }

}
