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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.service.UserService.UserGrantedAuthority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

@Controller
@Component("userController")
public class UserController extends BasicController implements UserDetailsService {
    
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    @Autowired
    private UserService userService;

    @PostConstruct
    public void init() throws IOException {
    }
    
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return get(username);
    }

    @RequestMapping(value = "/users/{userName}", method = { RequestMethod.POST, RequestMethod.PUT })
    @ResponseBody
    public UserObj save(@PathVariable String userName, @RequestBody UserObj user) {
        checkUserName(userName);
        
        user.setUsername(userName);

        // merge with existing user
        try {
            UserObj existing = get(userName);
            if (user.getPassword() == null)
                user.setPassword(existing.getPassword());
            if (user.getAuthorities() == null)
                user.setAuthorities(existing.getAuthorities());
        } catch (UsernameNotFoundException ex) {
            // that is OK, we create new
        }
        
        UserDetails details = userObjToDetails(user);
        userService.updateUser(details);
        
        return get(userName);
    }

    private void checkUserName(String userName) {
        if (userName == null || userName.isEmpty())
            throw new IllegalArgumentException();
    }
    
    @RequestMapping(value = "/users/{userName}", method = { RequestMethod.GET })
    @ResponseBody
    public UserObj get(@PathVariable String userName) throws UsernameNotFoundException {
        checkUserName(userName);
        
        UserDetails details = userService.loadUserByUsername(userName);
        UserObj user = userDetailsToObj(details);
        return user;
    }

    @RequestMapping(value = "/users", method = { RequestMethod.GET })
    @ResponseBody
    public List<UserObj> getAll() {
        List<UserObj> result = Lists.newArrayList();
        for (UserDetails details : userService.listUsers()) {
            result.add(userDetailsToObj(details));
        }
        return result;
    }

    @RequestMapping(value = "/users/{userName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void delete(@PathVariable String userName) {
        checkUserName(userName);
        
        userService.deleteUser(userName);
    }
    
    private static final String DISABLED_ROLE = "--disabled--";

    private UserDetails userObjToDetails(UserObj obj) {
        List<UserGrantedAuthority> detailRoles = Lists.newArrayList(obj.getAuthorities());
        if (obj.isDisabled()) {
            detailRoles.add(new UserGrantedAuthority(DISABLED_ROLE));
        }
        return new User(obj.getUsername(), obj.getPassword(), detailRoles);
    }
    
    private UserObj userDetailsToObj(UserDetails details) {
        UserObj obj = new UserObj();
        
        obj.setUsername(details.getUsername());
        obj.setPassword(details.getPassword());
        
        List<UserGrantedAuthority> roles = Lists.newArrayList();
        if (details.getAuthorities() != null) {
            for (GrantedAuthority a : details.getAuthorities()) {
                if (DISABLED_ROLE.equals(a.getAuthority())) {
                    obj.setDisabled(true);
                    continue;
                }
                roles.add(new UserGrantedAuthority(a.getAuthority()));
            }
        }
        
        obj.setAuthorities(roles);
        
        return obj;
    }
    
    public static class UserObj implements UserDetails {
        private static final long serialVersionUID = 1L;
        
        private String username;
        private String password;
        private List<UserGrantedAuthority> authorities;
        private boolean disabled;
        
        public UserObj() {
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String userName) {
            this.username = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public List<UserGrantedAuthority> getAuthorities() {
            return authorities;
        }

        public void setAuthorities(List<UserGrantedAuthority> authorities) {
            this.authorities = authorities;
        }

        public boolean isDisabled() {
            return disabled;
        }

        public void setDisabled(boolean disabled) {
            this.disabled = disabled;
        }

        @Override
        public boolean isAccountNonExpired() {
            return true;
        }

        @Override
        public boolean isAccountNonLocked() {
            return true;
        }

        @Override
        public boolean isCredentialsNonExpired() {
            return true;
        }

        @Override
        public boolean isEnabled() {
            return !disabled;
        }
    }

}
