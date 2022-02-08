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

package io.kyligence.kap.rest.util;

import static io.kyligence.kap.rest.util.CreateAdminUserUtils.PROFILE_DEFAULT;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;
import org.springframework.core.env.Environment;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.usergroup.NUserGroupManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitUserGroupUtils {

    public static void initUserGroups(Environment env) {
        String[] groupNames = new String[] { Constant.GROUP_ALL_USERS, Constant.ROLE_ADMIN, Constant.ROLE_MODELER,
                Constant.ROLE_ANALYST };
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NUserGroupManager userGroupManager = NUserGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
            if (userGroupManager.getAllGroups().isEmpty() && env.acceptsProfiles(PROFILE_DEFAULT)) {
                for (String groupName : groupNames) {
                    userGroupManager.add(groupName);
                    log.info("Init user group {}.", groupName);
                }
            }
            return null;
        }, UnitOfWork.GLOBAL_UNIT);

    }
}
