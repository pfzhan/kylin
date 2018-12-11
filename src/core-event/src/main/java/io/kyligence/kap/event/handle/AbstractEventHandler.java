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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;

import com.google.common.collect.Sets;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEventHandler implements EventHandler {

    public AbstractEventHandler() {
    }

    @Override
    public final void handle(EventContext eventContext) {

        try {
            boolean valid = checkBeforeHandle(eventContext);
            if (!valid) {
                log.info("handle {} later", eventContext);
                return;
            }
            doHandle(eventContext);
        } catch (Exception e) {
            //TODO: how to handle error? will there be errors?
            throw e;
        }
    }

    protected boolean checkBeforeHandle(EventContext eventContext) {
        Event event = eventContext.getEvent();
        checkNotNull(event);
        KylinConfig kylinConfig = eventContext.getConfig();
        String project = event.getProject();
        checkNotNull(project);
        checkNotNull(NProjectManager.getInstance(kylinConfig).getProject(project));
        val execManager = NExecutableManager.getInstance(kylinConfig, project);
        val runningCount = execManager.countByModelAndStatus(event.getModelName(),
                Sets.newHashSet(ExecutableState.RUNNING, ExecutableState.READY));
        log.debug("model {} has {} running jobs", event.getModelName(), runningCount);
        return runningCount == 0L;
    }

    protected EventDao getEventDao(String project, KylinConfig config) {
        return EventDao.getInstance(config, project);
    }

    protected NExecutableManager getExecutableManager(String project, KylinConfig config) {
        return NExecutableManager.getInstance(config, project);
    }

    protected abstract void doHandle(EventContext eventContext);

}
