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
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import io.kyligence.kap.rest.request.UserRequest;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.service.UserService.UserGrantedAuthority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

@Controller
@Component("kapUserController")
@RequestMapping(value = "/kapuser")
public class UserController extends BasicController implements UserDetailsService {

    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    @Autowired
    private UserService userService;

    private Pattern bcryptPattern;
    private BCryptPasswordEncoder pwdEncoder;

    @PostConstruct
    public void init() throws IOException {
        bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
        pwdEncoder = new BCryptPasswordEncoder();

        List<UserObj> all = listAllUsers();
        logger.info("All " + all.size() + " users");
        if (all.isEmpty()) {
            save("ADMIN", new UserObj("ADMIN", "KYLIN", "ROLE_MODELER", "ROLE_ANALYST", "ROLE_ADMIN"));
            save("ANALYST", new UserObj("ANALYST", "ANALYST", "ROLE_ANALYST"));
            save("MODELER", new UserObj("MODELER", "MODELER", "ROLE_MODELER"));
        }
        for (UserObj u : all)
            logger.info(u.toString());
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return get(username);
    }

    @RequestMapping(value = "/users/{userName}", method = { RequestMethod.POST, RequestMethod.PUT })
    @ResponseBody
    public UserObj save(@PathVariable("userName") String userName, @RequestBody UserObj user) {
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

        user.setPassword(pwdEncode(user.getPassword()));

        logger.info("Saving " + user);

        UserDetails details = userObjToDetails(user);
        userService.updateUser(details);

        return get(userName);
    }

    @RequestMapping(value = "/users/password", method = {RequestMethod.PUT })
    @ResponseBody
    public UserObj save( @RequestBody UserRequest user) {
        if(!isAdmin()&&!getPrincipal().equals(user.getUsername())){
            throw new IllegalStateException("Permission denied!");
        }
        checkUserName(user.getUsername());

        UserObj existing = get(user.getUsername());
        if(!isAdmin()&&!pwdEncoder.matches(user.getPassword(),existing.getPassword())){
            throw new IllegalStateException("Old password is not correct!");
        }

        existing.setPassword(pwdEncode(user.getNewPassword()));

        logger.info("update password for user " + user);

        UserDetails details = userObjToDetails(existing);
        userService.updateUser(details);

        return get(user.getUsername());
    }


    private String pwdEncode(String pwd) {
        if (bcryptPattern.matcher(pwd).matches())
            return pwd;

        return pwdEncoder.encode(pwd);
    }

    private void checkUserName(String userName) {
        if (userName == null || userName.isEmpty())
            throw new IllegalArgumentException();
    }

    @RequestMapping(value = "/users/{userName}", method = { RequestMethod.GET })
    @ResponseBody
    public UserObj get(@PathVariable("userName") String userName) throws UsernameNotFoundException {
        checkUserName(userName);

        UserDetails details = userService.loadUserByUsername(userName);
        UserObj user = userDetailsToObj(details);
        return user;
    }

    @RequestMapping(value = "/users", method = { RequestMethod.GET })
    @ResponseBody
    public List<UserObj> listAllUsers() {
        List<UserObj> result = Lists.newArrayList();
        for (UserDetails details : userService.listUsers()) {
            result.add(userDetailsToObj(details));
        }
        return result;
    }

    @RequestMapping(value = "/users/{userName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void delete(@PathVariable("userName") String userName) {
        checkUserName(userName);

        userService.deleteUser(userName);
    }

    @RequestMapping(value = "/userAuhtorities", method = { RequestMethod.GET })
    @ResponseBody
    public List<String> listAllAuthorities() {
        List<String> result = userService.listUserAuthorities();
        result.remove(DISABLED_ROLE);
        return result;
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

    private String getPrincipal(){
        String userName = null;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Object principal = authentication.getPrincipal();

        if (principal instanceof UserDetails) {
            userName = ((UserDetails)principal).getUsername();
        }else if (authentication.getDetails() instanceof UserDetails) {
            userName = ((UserDetails) authentication.getPrincipal()).getUsername();
        }
        else {
            userName = principal.toString();
        }
        return userName;
    }

    private boolean isAdmin(){
        boolean isAdmin = false;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if(authentication!=null){
            for (GrantedAuthority auth : authentication.getAuthorities()) {
                if (auth.getAuthority().equals(Constant.ROLE_ADMIN)) {
                    isAdmin = true;
                }
                break;
            }
        }
        return isAdmin;
    }

    public static class UserObj implements UserDetails {
        private static final long serialVersionUID = 1L;

        private String username;
        private String password;
        private List<UserGrantedAuthority> authorities;
        private boolean disabled;

        public UserObj() {
        }

        public UserObj(String username, String password, String... authorities) {
            this.username = username;
            this.password = password;
            this.authorities = Lists.newArrayList();
            for (String a : authorities) {
                this.authorities.add(new UserGrantedAuthority(a));
            }
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

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((username == null) ? 0 : username.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            UserObj other = (UserObj) obj;
            if (username == null) {
                if (other.username != null)
                    return false;
            } else if (!username.equals(other.username))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "UserObj [username=" + username + ", authorities=" + authorities + "]";
        }
    }

}
