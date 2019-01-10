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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

import io.kyligence.kap.event.event.ErrorEvent;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.manager.EventOrchestrator;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ErrorEventHandlerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testEventError() throws InterruptedException {

        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT);

        ErrorEvent event = new ErrorEvent();
        event.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event.setOwner("ADMIN");

        ErrorEvent event2 = new ErrorEvent();
        event2.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event2.setOwner("ADMIN");

        ErrorEvent event3 = new ErrorEvent();
        event3.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event3.setOwner("ADMIN");

        eventManager.post(event);

        eventManager.post(event2);

        eventManager.post(event3);

        Assert.assertEquals(3, eventDao.getEvents().size());

        val eventOrchestrator = new EventOrchestrator(DEFAULT_PROJECT, getTestConfig());

        var df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());

        val oldTime = System.currentTimeMillis();

        while (eventDao.getEvents().size() > 0) {

            Thread.sleep(1000);
            if (System.currentTimeMillis() - oldTime >= 100000) {
                Assert.fail("Didn't clean events!");
                break;
            }
        }

        eventOrchestrator.shutdown();

        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.BROKEN, df.getStatus());
        Assert.assertEquals(true, df.isEventError());

    }

}
