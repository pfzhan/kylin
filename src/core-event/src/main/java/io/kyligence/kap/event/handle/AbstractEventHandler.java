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


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.event.handle;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import org.apache.kylin.common.KylinConfig;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.EventStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.google.common.base.Preconditions.checkNotNull;
import static io.kyligence.kap.event.manager.EventManager.GLOBAL;

public abstract class AbstractEventHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEventHandler.class);

    public AbstractEventHandler(){
        EventOrchestratorManager.getInstance(KylinConfig.getInstanceFromEnv()).register(this);
    }

    @Override
    public final void handle(EventContext eventContext) throws Exception {

        try {
            onHandleStart(eventContext);

            Throwable throwable;
            do {
                try {
                    throwable = null;
                    doHandle(eventContext);
                } catch (Throwable e) {
                    logger.error("EventHandler doHandle error : " + e.getMessage(), e);
                    throwable = e;
                }
            } while (needRetry(eventContext, throwable));

            if (throwable != null) {
                throw new RuntimeException(throwable);
            }

            onHandleFinished(eventContext);

        } catch (Exception e) {
            onHandleError(eventContext, e);
            logger.error("handle error : " + e.getMessage(), e);
        }

    }

    private boolean needRetry(EventContext eventContext, Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        int retry = eventContext.getRetry();
        retry --;
        eventContext.setRetry(retry);
        return retry > 0;
    }

    private void onHandleError(EventContext eventContext, Exception e) throws Exception {
        Event event = eventContext.getEvent();
        event.setMsg(e.getMessage());
        event.setStatus(EventStatus.ERROR);
        getEventDao(eventContext).updateEvent(event);
    }

    protected void onHandleFinished(EventContext eventContext) throws Exception {
        Event event = eventContext.getEvent();
        event.setStatus(EventStatus.SUCCEED);
        getEventDao(eventContext).updateEvent(event);
    }

    protected void onHandleStart(EventContext eventContext) throws Exception {
        Event event = eventContext.getEvent();
        checkNotNull(event);
        KylinConfig kylinConfig = eventContext.getConfig();
        String project = event.getProject();
        checkNotNull(project);
        checkNotNull(NProjectManager.getInstance(kylinConfig).getProject(project));

        event.setStatus(EventStatus.RUNNING);
        getEventDao(eventContext).updateEvent(event);
    }

    protected EventDao getEventDao(EventContext eventContext) {
        String project = eventContext.getEvent().getProject();
        if (eventContext.getEvent().isGlobal()) {
            project = GLOBAL;
        }
        return EventDao.getInstance(eventContext.getConfig(), project);
    }

    protected abstract void doHandle(EventContext eventContext) throws Exception;

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        return this.getClass().equals(obj.getClass());
    }
}
