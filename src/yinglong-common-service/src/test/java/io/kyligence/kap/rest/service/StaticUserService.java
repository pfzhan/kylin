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

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kylin.rest.constant.Constant;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.user.ManagedUser;

public class StaticUserService extends OpenUserService {
    private List<ManagedUser> users = Lists.newArrayList();

    @PostConstruct
    public void init() {
        ManagedUser admin = new ManagedUser();
        admin.addAuthorities(Constant.ROLE_ADMIN);
        admin.setPassword("123456");
        admin.setUsername("admin");
        admin.setDisabled(false);
        admin.setLocked(false);
        users.add(admin);
        ManagedUser test = new ManagedUser();
        test.addAuthorities(Constant.ROLE_ANALYST);
        test.setPassword("123456");
        test.setUsername("test");
        test.setDisabled(false);
        test.setLocked(false);
        users.add(test);
    }

    @Override
    public List<ManagedUser> listUsers() {
        return users;
    }

    @Override
    public List<String> listAdminUsers() {
        List<String> admins = Lists.newArrayList();
        for (ManagedUser user : users) {
            if (user.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))) {
                admins.add(user.getUsername());
            }
        }
        return admins;
    }

    @Override
    public boolean userExists(String s) {
        for (ManagedUser user : users) {
            if (s.equals(user.getUsername())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public UserDetails loadUserByUsername(String s) {
        for (ManagedUser user : users) {
            if (s.equals(user.getUsername())) {
                return user;
            }
        }
        throw new UsernameNotFoundException(s);
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
        // do nothing
    }
}
