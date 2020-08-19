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

import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;

import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.acl.UserGroup;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.extern.slf4j.Slf4j;

import static org.apache.kylin.common.persistence.ResourceStore.USER_GROUP_ROOT;

@Slf4j
public class InitUserGroupUtils {
    public static void initUserGroups() {
        val rs = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        if (rs.exists(USER_GROUP_ROOT)) {
            return;
        }
        log.info("Init user group");
        UserGroup userGroup = new UserGroup();
        userGroup.add(Constant.GROUP_ALL_USERS);
        userGroup.add(Constant.ROLE_ADMIN);
        userGroup.add(Constant.ROLE_MODELER);
        userGroup.add(Constant.ROLE_ANALYST);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource(USER_GROUP_ROOT,
                    ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(userGroup)), System.currentTimeMillis(), -1L);
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }
}
