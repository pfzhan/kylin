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

package org.apache.kylin.rest.security;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.core.Authentication;

public class KylinAclPermissionEvaluator extends AclPermissionEvaluator {

    private PermissionFactory kylinPermissionFactory;

    public KylinAclPermissionEvaluator(AclService aclService, PermissionFactory permissionFactory) {
        super(aclService);
        super.setPermissionFactory(permissionFactory);
        this.kylinPermissionFactory = permissionFactory;
    }

    @Override
    public boolean hasPermission(Authentication authentication, Object targetDomainObject, Object permission) {
        ExternalAclProvider eap = ExternalAclProvider.getInstance();
        //because Transaction(project= ) need project name,transfer project name to prjInstance here
        if (!(targetDomainObject instanceof ProjectInstance) && targetDomainObject instanceof String) {
            targetDomainObject = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(targetDomainObject.toString());
        }
        if (eap == null)
            return super.hasPermission(authentication, targetDomainObject, permission);

        if (targetDomainObject == null) {
            return false;
        }

        AclEntity e = (AclEntity) targetDomainObject;
        return checkExternalPermission(eap, authentication, e.getClass().getSimpleName(), e.getId(), permission);
    }

    private boolean checkExternalPermission(ExternalAclProvider eap, Authentication authentication, String entityType,
            String entityUuid, Object permission) {

        String currentUser = authentication.getName();
        List<String> authorities = AclPermissionUtil.transformAuthorities(authentication.getAuthorities());
        List<Permission> kylinPermissions = resolveKylinPermission(permission);

        for (Permission p : kylinPermissions) {
            if (eap.checkPermission(currentUser, authorities, entityType, entityUuid, p))
                return true;
        }
        return false;
    }

    private List<Permission> resolveKylinPermission(Object permission) {
        if (permission instanceof Integer) {
            return Arrays.asList(kylinPermissionFactory.buildFromMask(((Integer) permission).intValue()));
        }

        if (permission instanceof Permission) {
            return Arrays.asList((Permission) permission);
        }

        if (permission instanceof Permission[]) {
            return Arrays.asList((Permission[]) permission);
        }

        if (permission instanceof String) {
            String permString = (String) permission;
            Permission p;

            try {
                p = kylinPermissionFactory.buildFromName(permString);
            } catch (IllegalArgumentException notfound) {
                p = kylinPermissionFactory.buildFromName(permString.toUpperCase());
            }

            if (p != null) {
                return Arrays.asList(p);
            }

        }
        throw new IllegalArgumentException("Unsupported permission: " + permission);
    }

    @Override
    public boolean hasPermission(Authentication authentication, Serializable targetId, String targetType,
            Object permission) {
        ExternalAclProvider eap = ExternalAclProvider.getInstance();
        if (eap == null)
            return super.hasPermission(authentication, targetId, targetType, permission);
        
        return checkExternalPermission(eap, authentication, targetType, targetId.toString(), permission);
    }
}
