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

package org.apache.kylin.rest.config.initialize;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.security.AdminUserSyncEventNotifier;
import org.apache.kylin.rest.service.UserAclService;
import org.apache.kylin.rest.util.SpringContext;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.epoch.EpochManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserAclListener {

    @Subscribe
    public void syncAdminUserAcl(AdminUserSyncEventNotifier event) {
        UserAclService userAclService = SpringContext.getBean(UserAclService.class);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isJobNode() && EpochManager.getInstance().checkEpochOwner(UnitOfWork.GLOBAL_UNIT)) {
            userAclService.syncAdminUserAcl(event.getAdminUserList(), event.isUseEmptyPermission());
        } else {
            userAclService.remoteSyncAdminUserAcl(event);
        }
    }

}
