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
package io.kyligence.kap.event;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.event.handle.AbstractEventHandler;
import io.kyligence.kap.event.handle.AddSegmentHandler;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventStatus;
import io.kyligence.kap.event.model.PostModelSemanticUpdateEvent;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSemanticTest extends AbstractMVCIntegrationTestCase {

    public static final String DEFAULT_PROJECT = "default";
    public static final String MODEL_NAME = "nmodel_basic";

    @Before
    public void setupHandlers() {
        EventOrchestratorManager.destroyInstance();
        val scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(AbstractEventHandler.class));
        for (BeanDefinition component : scanner.findCandidateComponents("io.kyligence.kap")) {
            try {
                if (component.isAbstract()) {
                    continue;
                }
                val clazz = Class.forName(component.getBeanClassName());
                if (clazz.equals(AddSegmentHandler.class)) {
                    new MockAddSegmentHandler();
                } else {
                    clazz.newInstance();
                }
                log.debug("new handler {}", component.getBeanClassName());
            } catch (Exception e) {
                log.debug("cannot construct {}", component.getBeanClassName());
            }
        }
        System.setProperty("kylin.job.scheduler.poll-interval-second", "3");
    }

    @Test
    public void testSemanticChangedHappy() throws Exception {
        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        changeModelRequest();

        val eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val firstStepEvents = eventDao.getEvents();
        Assert.assertEquals(1, firstStepEvents.size());

        var df = dfManager.getDataflowByModelName(MODEL_NAME);
        Assert.assertTrue(df.isReconstructing());

        val runningEventSize = waitForEventFinished(3);
        df = dfManager.getDataflowByModelName(MODEL_NAME);
        Assert.assertEquals(0, runningEventSize);
        val allEvents = eventDao.getEvents();
        allEvents.sort(Comparator.comparingLong(Event::getCreateTimeNanosecond));
        val addEvent = (AddCuboidEvent) allEvents.get(1);
        Assert.assertTrue(CollectionUtils.isEqualCollection(addEvent.getLayoutIds(),
                Arrays.<Long> asList(1000001L, 1L, 1001L, 1002L, 2001L, 3001L, 20000001001L)));
        Assert.assertFalse(df.isReconstructing());

    }

    @Test
    public void testSkipAddSegmentStep() throws Exception {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val prevEvent = new AddSegmentEvent();
        prevEvent.setJobId("job123");
        prevEvent.setProject(DEFAULT_PROJECT);
        prevEvent.setModelName(MODEL_NAME);
        prevEvent.setCubePlanName("ncube_basic");
        prevEvent.setApproved(true);
        eventManager.post(prevEvent);

        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        changeModelRequest();
        val eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val firstStepEvents = eventDao.getEvents();
        Assert.assertEquals(2, firstStepEvents.size());

        waitForEventFinished(6);
        val events = eventDao.getEvents();
        events.sort(Comparator.comparingLong(e -> e.getCreateTimeNanosecond()));
        log.debug("events are {}", events);
        Assert.assertEquals(6, events.size());
        Assert.assertTrue(events.get(4) instanceof PostModelSemanticUpdateEvent);
        Assert.assertTrue(events.get(5) instanceof AddSegmentEvent);
    }

    private void changeModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = modelManager.getDataModelDesc(MODEL_NAME);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(DEFAULT_PROJECT);
        request.setName(MODEL_NAME);
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.tomb)
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs());
        request.setAllNamedColumns(model.getAllNamedColumns().stream()
                .filter(c -> c.status == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList()));
        request.setJoinTables(
                request.getJoinTables().stream().peek(j -> j.getJoin().setType("inner")).collect(Collectors.toList()));
        val result = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    private List<DefaultChainedExecutable> genMockJobs(int size, ExecutableState state) {
        List<DefaultChainedExecutable> jobs = Lists.newArrayList();
        if (size <= 0) {
            return jobs;
        }
        for (int i = 0; i < size; i++) {
            DefaultChainedExecutable job = Mockito.spy(DefaultChainedExecutable.class);
            Mockito.doReturn(state).when(job).getStatus();
            jobs.add(job);
        }
        return jobs;
    }

    private long waitForEventFinished(int expectedSize) throws Exception {
        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        List<Event> events = Lists.newArrayList();
        val startTime = System.currentTimeMillis();
        while (true) {
            int finishedEventNum = 0;
            events = eventDao.getEvents();
            for (Event event : events) {
                EventStatus status = event.getStatus();
                if (status.equals(EventStatus.SUCCEED) || status.equals(EventStatus.ERROR)) {
                    finishedEventNum++;
                }
            }
            log.debug("finished {}, all {}", finishedEventNum, events.size());
            if (finishedEventNum == events.size() && finishedEventNum == expectedSize) {
                break;
            }
            if (System.currentTimeMillis() - startTime > 20 * 1000) {
                break;
            }
            Thread.sleep(5000);
        }
        return events.stream().filter(e -> e.getStatus() == EventStatus.READY).count();
    }

}
