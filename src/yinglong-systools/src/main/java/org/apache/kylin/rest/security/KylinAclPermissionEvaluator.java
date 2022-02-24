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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.core.Authentication;

import io.kyligence.kap.metadata.project.NProjectManager;

public class KylinAclPermissionEvaluator extends AclPermissionEvaluator {

    private PermissionFactory permissionFactory;

    public KylinAclPermissionEvaluator(AclService aclService, PermissionFactory permissionFactory) {
        super(aclService);
        super.setPermissionFactory(permissionFactory);
        this.permissionFactory = permissionFactory;
    }

    @Override
    public boolean hasPermission(Authentication authentication, Object targetDomainObject, Object permission) {
        if (Objects.isNull(targetDomainObject)) {
            return false;
        }

        //because Transaction(project= ) need project name, transfer project name to prjInstance here
        if (targetDomainObject instanceof String) {
            targetDomainObject = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getProject(targetDomainObject.toString());
        }

        ExternalAclProvider eap = ExternalAclProvider.getInstance();
        if (Objects.isNull(eap)) {
            return super.hasPermission(authentication, targetDomainObject, permission);
        }

        AclEntity e = (AclEntity) targetDomainObject;
        return checkExternalPermission(eap, authentication, e.getClass().getSimpleName(), e.getId(), permission);
    }

    private boolean checkExternalPermission(ExternalAclProvider eap, Authentication authentication, String entityType,
            String entityUuid, Object permission) {

        String currentUser = authentication.getName();
        List<String> authorities = AclPermissionUtil.transformAuthorities(authentication.getAuthorities());
        List<Permission> permissions = resolveKylinPermission(permission);

        for (Permission p : permissions) {
            if (eap.checkPermission(currentUser, authorities, entityType, entityUuid, p)) {
                return true;
            }
        }
        return false;
    }

    private List<Permission> resolveKylinPermission(Object permission) {
        if (permission instanceof Integer) {
            return Arrays.asList(permissionFactory.buildFromMask(((Integer) permission).intValue()));
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
                p = permissionFactory.buildFromName(permString);
            } catch (IllegalArgumentException notfound) {
                p = permissionFactory.buildFromName(permString.toUpperCase(Locale.ROOT));
            }

            if (Objects.nonNull(p)) {
                return Collections.singletonList(p);
            }

        }
        throw new IllegalArgumentException("Unsupported permission: " + permission);
    }

    @Override
    public boolean hasPermission(Authentication authentication, Serializable targetId, String targetType,
            Object permission) {
        ExternalAclProvider eap = ExternalAclProvider.getInstance();
        if (Objects.isNull(eap)) {
            return super.hasPermission(authentication, targetId, targetType, permission);
        }

        return checkExternalPermission(eap, authentication, targetType, targetId.toString(), permission);
    }
}
