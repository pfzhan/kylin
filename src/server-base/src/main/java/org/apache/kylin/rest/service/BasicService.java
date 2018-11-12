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

package org.apache.kylin.rest.service;

import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffsetManager;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.acl.TableACLManager;

import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryFilterRuleManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

public abstract class BasicService {

    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public NTableMetadataManager getTableManager(String project) {
        return NTableMetadataManager.getInstance(getConfig(), project);
    }

    public NDataModelManager getDataModelManager(String project) {
        return NDataModelManager.getInstance(getConfig(), project);
    }

    public NDataflowManager getDataflowManager(String project) {
        return NDataflowManager.getInstance(getConfig(), project);
    }

    public NCubePlanManager getCubePlanManager(String project) {
        return NCubePlanManager.getInstance(getConfig(), project);
    }

    public NDataLoadingRangeManager getDataLoadingRangeManager(String project) {
        return NDataLoadingRangeManager.getInstance(getConfig(), project);
    }

    public EventManager getEventManager(String project) {
        return EventManager.getInstance(getConfig(), project);
    }

    public NProjectManager getProjectManager() {
        return NProjectManager.getInstance(getConfig());
    }

    public TableACLManager getTableACLManager() {
        return TableACLManager.getInstance(getConfig());
    }

    public EventDao getEventDao(String project) {
        return EventDao.getInstance(getConfig(), project);
    }

    public NExecutableManager getExecutableManager(String project) {
        return NExecutableManager.getInstance(getConfig(), project);
    }

    public QueryFilterRuleManager getQueryFilterRuleManager(String project) {
        return QueryFilterRuleManager.getInstance(getConfig(), project);
    }

    protected String getUsername() {
        String username = SecurityContextHolder.getContext().getAuthentication().getName();
        if (StringUtils.isEmpty(username)) {
            username = "";
        }
        return username;
    }

    public QueryHistoryDAO getQueryHistoryManager() {
        return QueryHistoryDAO.getInstance(getConfig());
    }

    public QueryHistoryTimeOffsetManager getQHTimeOffsetManager() {
        return QueryHistoryTimeOffsetManager.getInstance(getConfig());
    }
}
