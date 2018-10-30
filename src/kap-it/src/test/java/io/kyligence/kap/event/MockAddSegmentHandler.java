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

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.event.handle.AbstractEventHandler;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.EventContext;
import lombok.val;

public class MockAddSegmentHandler extends AbstractEventHandler {
    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        AddSegmentEvent event = (AddSegmentEvent) eventContext.getEvent();

        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        val dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        val eventManager = EventManager.getInstance(kylinConfig, project);

        NDataflow df = dfMgr.getDataflow(event.getCubePlanName());
        // repost event
        if (df.isReconstructing()) {
            val newEvent = new AddSegmentEvent();
            newEvent.setModelName(event.getModelName());
            newEvent.setProject(event.getProject());
            newEvent.setApproved(event.isApproved());
            newEvent.setCubePlanName(event.getCubePlanName());
            newEvent.setSegmentIds(event.getSegmentIds());
            newEvent.setAddedInfo(event.getAddedInfo());
            eventManager.post(newEvent);
        }
        Thread.sleep(2000);
    }

    @Override
    public Class<?> getEventClassType() {
        return AddSegmentEvent.class;
    }
}
