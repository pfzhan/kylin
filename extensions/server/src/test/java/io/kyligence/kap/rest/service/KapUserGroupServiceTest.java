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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.UserService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Lists;

public class KapUserGroupServiceTest extends ServiceTestBase {
    @Autowired
    @Qualifier("userGroupService")
    private KapUserGroupService kapUserGroupService;

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Test
    public void testBasic() throws IOException {
        //test group add and get
        kapUserGroupService.addGroup("allUsers");
        kapUserGroupService.addGroup("g1");
        kapUserGroupService.addGroup("g2");
        kapUserGroupService.addGroup("g3");
        Assert.assertEquals(Lists.newArrayList("allUsers", "g1", "g2", "g3"), kapUserGroupService.getAllUserGroups());

        //test modify users in user group
        for (int i = 1; i <= 6; i++) {
            userService.updateUser(new ManagedUser("u" + i, "kylin", false, "allUsers"));
        }
        kapUserGroupService.modifyGroupUsers("g1", Lists.newArrayList("u1", "u3", "u5"));
        kapUserGroupService.modifyGroupUsers("g2", Lists.newArrayList("u2", "u4", "u6"));

        Assert.assertEquals(Lists.newArrayList("u1", "u3", "u5"), getUsers("g1"));
        Assert.assertEquals(Lists.newArrayList("u2", "u4", "u6"), getUsers("g2"));
        Assert.assertEquals(0, kapUserGroupService.getGroupMembersByName("g3").size());
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority("allUsers"), new SimpleGrantedAuthority("g1")), userService.loadUserByUsername("u1").getAuthorities());
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority("allUsers"), new SimpleGrantedAuthority("g2")), userService.loadUserByUsername("u2").getAuthorities());

        kapUserGroupService.modifyGroupUsers("g1", Lists.newArrayList("u3", "u5"));
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority("allUsers")), userService.loadUserByUsername("u1").getAuthorities());

        //test delete
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority("allUsers"), new SimpleGrantedAuthority("g1")), userService.loadUserByUsername("u3").getAuthorities());
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority("allUsers"), new SimpleGrantedAuthority("g1")), userService.loadUserByUsername("u5").getAuthorities());
        kapUserGroupService.deleteGroup("g1");
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority("allUsers")), userService.loadUserByUsername("u3").getAuthorities());
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority("allUsers")), userService.loadUserByUsername("u5").getAuthorities());
    }

    private List<String> getUsers(String groupName) throws IOException {
        List<String> users = new ArrayList<>();
        for (ManagedUser u : kapUserGroupService.getGroupMembersByName(groupName)) {
            users.add(u.getUsername());
        }
        return users;
    }
}
