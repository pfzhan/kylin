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

package io.kyligence.kap.rest.service;

import java.util.Arrays;
import java.util.Set;

import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;

public class CaseInsensitiveUserGroupServiceTest extends NLocalFileMetadataTestCase {

    private CaseInsensitiveUserGroupService userGroupService;

    private CaseInsensitiveKylinUserService kylinUserService;

    @Before
    public void setup() {
        createTestMetadata();
        overwriteSystemProp("kylin.metadata.key-case-insensitive", "true");
        kylinUserService = Mockito.spy(new CaseInsensitiveKylinUserService());
        userGroupService = Mockito.spy(new CaseInsensitiveUserGroupService());
        ReflectionTestUtils.setField(userGroupService, "userService", kylinUserService);
        NKylinUserManager userManager = NKylinUserManager.getInstance(getTestConfig());
        userManager.update(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testListUserGroups() {
        Set<String> userGroups = userGroupService.listUserGroups("ADMIN");
        Assert.assertEquals(4, userGroups.size());
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_MODELER));
        Assert.assertTrue(userGroups.contains(Constant.GROUP_ALL_USERS));

        userGroups = userGroupService.listUserGroups("AdMiN");
        Assert.assertEquals(4, userGroups.size());
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_MODELER));
        Assert.assertTrue(userGroups.contains(Constant.GROUP_ALL_USERS));

        userGroups = userGroupService.listUserGroups("NOTEXISTS");
        Assert.assertEquals(0, userGroups.size());
    }
}