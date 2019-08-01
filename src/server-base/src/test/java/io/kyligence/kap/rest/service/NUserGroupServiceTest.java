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

import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.service.UserService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.user.ManagedUser;

public class NUserGroupServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("nUserGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Test
    public void testBasic() throws IOException {
        for (String group : userGroupService.getAllUserGroups()) {
            userGroupService.deleteGroup(group);
        }
        //test group add and get
//        userGroupService.addGroup(GROUP_ALL_USERS);
        userGroupService.addGroup("g1");
        userGroupService.addGroup("g2");
        userGroupService.addGroup("g3");
        Assert.assertEquals(Lists.newArrayList("g1", "g2", "g3"), userGroupService.getAllUserGroups());
        Assert.assertEquals(Lists.newArrayList("g1", "g2", "g3"), userGroupService.getAuthoritiesFilterByGroupName("G"));
        Assert.assertEquals(Lists.newArrayList("g1"), userGroupService.getAuthoritiesFilterByGroupName("g1"));

        // test add a existing user group
        try {
            userGroupService.addGroup("g1");
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, group:g1 already exists", e.getCause().getCause().getMessage());
        }

        //test modify users in user group
        for (int i = 1; i <= 6; i++) {
            userService.updateUser(new ManagedUser("u" + i, "kylin", false));
        }
        userGroupService.modifyGroupUsers("g1", Lists.newArrayList("u1", "u3", "u5"));
        userGroupService.modifyGroupUsers("g2", Lists.newArrayList("u2", "u4", "u6"));

        Assert.assertEquals(Lists.newArrayList("u1", "u3", "u5"), getUsers("g1"));
        Assert.assertEquals(Lists.newArrayList("u2", "u4", "u6"), getUsers("g2"));
        Assert.assertEquals(0, userGroupService.getGroupMembersByName("g3").size());
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g1")),
                userService.loadUserByUsername("u1").getAuthorities());
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g2")),
                userService.loadUserByUsername("u2").getAuthorities());

        userGroupService.modifyGroupUsers("g1", Lists.newArrayList("u3", "u5"));
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS)),
                userService.loadUserByUsername("u1").getAuthorities());

        //test delete
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g1")),
                userService.loadUserByUsername("u3").getAuthorities());
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g1")),
                userService.loadUserByUsername("u5").getAuthorities());
        userGroupService.deleteGroup("g1");
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS)),
                userService.loadUserByUsername("u3").getAuthorities());
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS)),
                userService.loadUserByUsername("u5").getAuthorities());

        Map<String, List<String>> result = userGroupService.getUserAndUserGroup();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(9, result.get("user").size());
        Assert.assertEquals(2, result.get("group").size());
        Assert.assertEquals("g2", result.get("group").get(0));
        Assert.assertEquals("g3", result.get("group").get(1));
    }

    private List<String> getUsers(String groupName) throws IOException {
        List<String> users = new ArrayList<>();
        for (ManagedUser u : userGroupService.getGroupMembersByName(groupName)) {
            users.add(u.getUsername());
        }
        return users;
    }
}
