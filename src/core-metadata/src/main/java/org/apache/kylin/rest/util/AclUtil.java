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

package org.apache.kylin.rest.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.KylinAclPermissionEvaluator;
import org.apache.kylin.rest.service.AclService;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.alibaba.nacos.api.utils.StringUtils;

@Lazy
@Component("aclUtil")
public class AclUtil {
    String getCurrentUserName() {
        return SecurityContextHolder.getContext().getAuthentication().getName();
    }

    //such method MUST NOT be called from within same class
    //do not change public to package private
    @PreAuthorize(Constant.ACCESS_POST_FILTER_READ)
    public boolean hasProjectReadPermission(ProjectInstance project) {
        return true;
    }

    @PreAuthorize(Constant.ACCESS_POST_FILTER_READ_FOR_DATA_PERMISSION_SEPARATE)
    public boolean hasProjectDataQueryPermission(ProjectInstance project) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            // PreAuthorize does not work in UT. So let's make an equivalent implementation for it.
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            KylinAclPermissionEvaluator evaluator = new KylinAclPermissionEvaluator(new AclService(),
                    new AclPermissionFactory());
            if (!evaluator.hasPermission(auth, project, AclPermission.DATA_QUERY)) {
                throw new AccessDeniedException(StringUtils.EMPTY);
            }
        }
        return true;
    }

    @PreAuthorize(Constant.ACCESS_CAN_PROJECT_OPERATION)
    public boolean hasProjectOperationPermission(ProjectInstance project) {
        return true;
    }

    @PreAuthorize(Constant.ACCESS_CAN_PROJECT_WRITE)
    public boolean hasProjectWritePermission(ProjectInstance project) {
        return true;
    }

    @PreAuthorize(Constant.ACCESS_CAN_PROJECT_ADMIN)
    public boolean hasProjectAdminPermission(ProjectInstance project) {
        return true;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public boolean checkIsGlobalAdmin() {
        return true;
    }

    @PreAuthorize(Constant.ACCESS_CAN_PROJECT_OPERATION_DESIGN)
    public boolean hasProjectOperationDesignPermission(ProjectInstance project, boolean isIndexEnableOperatorDesign) {
        return true;
    }
}
