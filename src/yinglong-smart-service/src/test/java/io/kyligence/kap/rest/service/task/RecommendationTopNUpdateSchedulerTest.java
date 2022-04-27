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

package io.kyligence.kap.rest.service.task;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.LogOutputTestCase;
import io.kyligence.kap.rest.service.RawRecService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecommendationTopNUpdateSchedulerTest extends LogOutputTestCase {

    private final RawRecService rawRecService = Mockito.mock(RawRecService.class);
    private final RecommendationTopNUpdateScheduler scheduler = Mockito.spy(new RecommendationTopNUpdateScheduler());
    private static final String PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        super.createTestMetadata();
        ReflectionTestUtils.setField(scheduler, "rawRecService", rawRecService);
    }

    @After
    public void tearDown() throws Exception {
        super.cleanupTestMetadata();
        scheduler.close();
    }

    @Test
    public void testSaveTimeFail() {
        overwriteSystemProp("kylin.smart.update-topn-time-gap", "1000");
        overwriteSystemProp("kylin.smart.frequency-rule-enable", "false");
        Mockito.doNothing().when(rawRecService).updateCostsAndTopNCandidates(PROJECT);
        Mockito.doThrow(RuntimeException.class).doCallRealMethod().when(scheduler).saveTaskTime(PROJECT);
        scheduler.addProject(PROJECT);
        await().atMost(3, TimeUnit.SECONDS)
                .until(() -> containsLog("Updating default cost and topN recommendations finished."));
    }

    @Test
    public void testSchedulerTask() {
        ReflectionTestUtils.setField(scheduler, "rawRecService", rawRecService);
        overwriteSystemProp("kylin.smart.update-topn-time-gap", "1000");
        overwriteSystemProp("kylin.smart.frequency-rule-enable", "false");
        Mockito.doNothing().when(rawRecService).updateCostsAndTopNCandidates(PROJECT);
        scheduler.addProject(PROJECT);
        await().atMost(3, TimeUnit.SECONDS)
                .until(() -> containsLog("Updating default cost and topN recommendations finished."));
    }
}