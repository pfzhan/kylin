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

import io.kyligence.kap.event.model.Event;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;

import io.kyligence.kap.event.manager.EventManager;
import lombok.val;

public interface DeriveEventMixin {

    default void fireEvent(Event newEvent, Event originEvent, KylinConfig kylinConfig)
            throws PersistentException {
        val eventManager = EventManager.getInstance(kylinConfig, originEvent.getProject());
        val eventAutoApproved = kylinConfig.getEventAutoApproved();

        newEvent.setApproved(eventAutoApproved);
        newEvent.setProject(originEvent.getProject());
        newEvent.setModelName(originEvent.getModelName());
        newEvent.setCubePlanName(originEvent.getCubePlanName());
        newEvent.setSegmentRange(originEvent.getSegmentRange());
        newEvent.setParentId(originEvent.getId());
        eventManager.post(newEvent);
    }

}
