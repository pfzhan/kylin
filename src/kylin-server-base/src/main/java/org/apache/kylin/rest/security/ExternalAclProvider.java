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

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.springframework.security.acls.model.Permission;

/**
 */
abstract public class ExternalAclProvider {

    private volatile static ExternalAclProvider singleton = null;

    public static ExternalAclProvider getInstance() {
        if (singleton == null) {
            synchronized (ExternalAclProvider.class) {
                if (singleton == null) {
                    String cls = KylinConfig.getInstanceFromEnv().getExternalAclProvider();
                    if (!StringUtils.isBlank(cls)) {
                        singleton = (ExternalAclProvider) ClassUtil.newInstance(cls);
                        singleton.init();
                    }
                }
            }
        }
        return singleton;
    }

    // ============================================================================

    public final static String ADMINISTRATION = "ADMIN";
    public final static String MANAGEMENT = "MANAGEMENT";
    public final static String OPERATION = "OPERATION";
    public final static String READ = "QUERY";
    
    // used by ranger ExternalAclProvider
    public static String transformPermission(Permission p) {
        String permString = null;
        if (AclPermission.ADMINISTRATION.equals(p)) {
            permString = ADMINISTRATION;
        } else if (AclPermission.MANAGEMENT.equals(p)) {
            permString = MANAGEMENT;
        } else if (AclPermission.OPERATION.equals(p)) {
            permString = OPERATION;
        } else if (AclPermission.READ.equals(p)) {
            permString = READ;
        } else {
            permString = p.getPattern();
        }
        return permString;
    }

    // ============================================================================
    
    abstract public void init();

    /**
     * Checks if a user has permission on an entity.
     * 
     * @param user
     * @param userRoles
     * @param entityType String constants defined in AclEntityType 
     * @param entityUuid
     * @param permission
     * 
     * @return true if has permission
     */
    abstract public boolean checkPermission(String user, List<String> userRoles, //
            String entityType, String entityUuid, Permission permission);

    /**
     * Returns all granted permissions on specified entity.
     * 
     * @param entityType String constants defined in AclEntityType
     * @param entityUuid
     * @return a list of (user/role, permission)
     */
    abstract public List<Pair<String, AclPermission>> getAcl(String entityType, String entityUuid);

}
