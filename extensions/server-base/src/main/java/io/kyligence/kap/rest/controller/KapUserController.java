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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

import io.kyligence.kap.rest.request.UserRequest;
import io.kyligence.kap.rest.security.KapAuthenticationManager;
import io.kyligence.kap.rest.security.KapAuthenticationManager.UserObj;

@Controller
@Component("kapUserController")
@RequestMapping(value = "/kap/user")
public class KapUserController extends BasicController implements UserDetailsService {

    private static final Logger logger = LoggerFactory.getLogger(KapUserController.class);

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    private KapAuthenticationManager kapAuthenticationManager;

    private Pattern passwordPattern;
    private Pattern bcryptPattern;
    private BCryptPasswordEncoder pwdEncoder;
    private static final UserGrantedAuthority ADMIN_AUTH = new UserGrantedAuthority(Constant.ROLE_ADMIN);
    private static final UserGrantedAuthority ANALYST_AUTH = new UserGrantedAuthority(Constant.ROLE_ANALYST);
    private static final UserGrantedAuthority MODELER_AUTH = new UserGrantedAuthority(Constant.ROLE_MODELER);

    @PostConstruct
    public void init() throws IOException {
        passwordPattern = Pattern.compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");
        bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
        pwdEncoder = new BCryptPasswordEncoder();

        List<UserObj> all = listAllUsers();
        logger.info("All " + all.size() + " users");
        if (all.isEmpty()) {
            save("ADMIN", new UserObj("ADMIN", "KYLIN", true, Constant.ROLE_ADMIN, Constant.ROLE_ANALYST, Constant.ROLE_MODELER));
            save("ANALYST", new UserObj("ANALYST", "ANALYST", true, Constant.ROLE_ANALYST));
            save("MODELER", new UserObj("MODELER", "MODELER", true, Constant.ROLE_MODELER, Constant.ROLE_MODELER));
        }

        kapAuthenticationManager.addUser(all);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return get(username);
    }

    @RequestMapping(value = "/{userName}", method = { RequestMethod.POST, RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public UserObj save(@PathVariable("userName") String userName, @RequestBody UserObj user) {
        checkUserName(userName);

        user.setUsername(userName);

        if (!user.isDefaultPassword()) {
            if (!checkPasswordLength(user.getPassword())) {
                throw new IllegalStateException("The password should contain more than 8 characters!");
            }

            if (!checkPasswordCharacter(user.getPassword())) {
                throw new IllegalStateException("The password should contain at least one numbers, letters and special characters（~!@#$%^&*(){}|:\"<>?[];\\'\\,./`)");
            }
        }

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
        kapAuthenticationManager.addUser(user);

        return get(userName);
    }


    @RequestMapping(value = "/password", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public UserObj save(@RequestBody UserRequest user) {
        if (!isAdmin() && !getPrincipal().equals(user.getUsername())) {
            throw new IllegalStateException("Permission denied!");
        }
        checkUserName(user.getUsername());

        if (!checkPasswordLength(user.getNewPassword())) {
            throw new IllegalStateException("The password should contain more than 8 characters!");
        }

        if (!checkPasswordCharacter(user.getNewPassword())) {
            throw new IllegalStateException("The password should contain at least one numbers, letters and special characters（~!@#$%^&*(){}|:\"<>?[];\\'\\,./`)");
        }

        UserObj existing = get(user.getUsername());
        if (!isAdmin() && !pwdEncoder.matches(user.getPassword(), existing.getPassword())) {
            throw new IllegalStateException("Old password is not correct!");
        }

        existing.setPassword(pwdEncode(user.getNewPassword()));
        existing.setDefaultPassword(false);

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

    private boolean checkPasswordLength(String password) {
        if (password == null || password.length() < 8)
            return false;
        return true;
    }

    private boolean checkPasswordCharacter(String password) {
        return passwordPattern.matcher(password).matches();
    }

    @RequestMapping(value = "/{userName}", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public UserObj get(@PathVariable("userName") String userName) throws UsernameNotFoundException {
        checkUserName(userName);

        UserDetails details = userService.loadUserByUsername(userName);
        UserObj user = userDetailsToObj(details);
        return user;
    }

    @RequestMapping(value = "/users", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<UserObj> listAllUsers() throws IOException {
        List<UserObj> result = Lists.newArrayList();
        for (UserDetails details : userService.listUsers()) {
            result.add(userDetailsToObj(details));
        }
        return result;
    }


    @RequestMapping(value = "/{userName}", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public void delete(@PathVariable("userName") String userName) {
        checkUserName(userName);
        kapAuthenticationManager.removeUser(userName);

        userService.deleteUser(userName);
    }


    @RequestMapping(value = "/userAuhtorities", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<String> listAllAuthorities() throws IOException {
        List<String> result = userService.listUserAuthorities();
        result.remove(DISABLED_ROLE);
        return result;
    }


    private static final String DISABLED_ROLE = "--disabled--";

    private UserDetails userObjToDetails(UserObj obj) {
        List<UserGrantedAuthority> detailRoles = Lists.newArrayList(obj.getAuthorities());
        if (detailRoles.contains(ADMIN_AUTH)) {
            if (!detailRoles.contains(MODELER_AUTH)) {
                logger.info("For ADMIN authority, add MODELER authority automatically");
                detailRoles.add(MODELER_AUTH);
            }

        }
        if (detailRoles.contains(MODELER_AUTH)) {
            if (!detailRoles.contains(ANALYST_AUTH)) {
                logger.info("For MODELER authority, add ANALYST authority automatically");
                detailRoles.add(ANALYST_AUTH);
            }
        }
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

    private String getPrincipal() {
        String userName = null;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Object principal = authentication.getPrincipal();

        if (principal instanceof UserDetails) {
            userName = ((UserDetails) principal).getUsername();
        } else if (authentication.getDetails() instanceof UserDetails) {
            userName = ((UserDetails) authentication.getPrincipal()).getUsername();
        } else {
            userName = principal.toString();
        }
        return userName;
    }

    private boolean isAdmin() {
        boolean isAdmin = false;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null) {
            for (GrantedAuthority auth : authentication.getAuthorities()) {
                if (auth.getAuthority().equals(Constant.ROLE_ADMIN)) {
                    isAdmin = true;
                }
                break;
            }
        }
        return isAdmin;
    }

}
