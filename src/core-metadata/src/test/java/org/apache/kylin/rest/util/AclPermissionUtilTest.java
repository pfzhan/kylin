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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.CompositeAclPermission;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.acls.model.Permission;
import org.springframework.test.util.ReflectionTestUtils;

public class AclPermissionUtilTest {

    @Test
    public void testNewFileCreation() {
        Assert.assertThrows(KylinException.class, () -> ReflectionTestUtils.invokeMethod(AclPermissionUtil.class,
                "checkIfAllowedProjectAdminGrantAcl", true));
        Assert.assertThrows(KylinException.class, () -> ReflectionTestUtils.invokeMethod(AclPermissionUtil.class,
                "checkIfAllowedProjectAdminGrantAcl", false));
    }

    @Test
    public void testAddExtPermission() {
        Permission permission = AclPermission.OPERATION;
        Assert.assertFalse(AclPermissionUtil.hasExtPermission(permission));
        Permission extPermission = AclPermission.DATA_QUERY;
        Assert.assertTrue(
                AclPermissionUtil.hasExtPermission(AclPermissionUtil.addExtPermission(permission, extPermission)));

        Permission compositePermission = new CompositeAclPermission(AclPermission.OPERATION);
        Assert.assertFalse(AclPermissionUtil.hasExtPermission(compositePermission));
        Assert.assertTrue(AclPermissionUtil
                .hasExtPermission(AclPermissionUtil.addExtPermission(compositePermission, extPermission)));
    }
}
