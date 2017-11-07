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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.PasswdChangeRequest;
import io.kyligence.kap.rest.service.UserGroupService;
import io.kyligence.kap.rest.util.ACLOperationUtil;

@Controller
@Component("kapUserController")
@RequestMapping(value = "/kap/user")
public class KapUserController extends BasicController implements UserDetailsService {

    private static final Logger logger = LoggerFactory.getLogger(KapUserController.class);

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("userGroupService")
    private UserGroupService userGroupService;

    private Pattern passwordPattern;
    private Pattern bcryptPattern;
    private BCryptPasswordEncoder pwdEncoder;
    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    @PostConstruct
    public void init() throws IOException {
        passwordPattern = Pattern.compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");
        bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
        pwdEncoder = new BCryptPasswordEncoder();

        List<ManagedUser> all = listAllUsers();
        logger.info("All " + all.size() + " users");
        if (all.isEmpty() && "testing".equals(System.getProperty("spring.profiles.active"))) {
            save("ADMIN", new ManagedUser("ADMIN", "KYLIN", true, Constant.ROLE_ADMIN, Constant.GROUP_ALL_USERS));
            save("ANALYST", new ManagedUser("ANALYST", "ANALYST", true, Constant.GROUP_ALL_USERS));
            save("MODELER", new ManagedUser("MODELER", "MODELER", true, Constant.GROUP_ALL_USERS));
        }

    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return get(username);
    }

    @RequestMapping(value = "/{userName}", method = { RequestMethod.POST, RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public ManagedUser save(@PathVariable("userName") String userName, @RequestBody ManagedUser user) {
        KapMessage msg = KapMsgPicker.getMsg();

        if (!"testing".equals(System.getProperty("spring.profiles.active"))) {
            throw new BadRequestException(msg.getUSER_EDIT_NOT_ALLOWED());
        }

        if (StringUtils.equals(getPrincipal(), user.getUsername()) && user.isDisabled()) {
            throw new ForbiddenException(msg.getSELF_DISABLE_FORBIDDEN());
        }

        checkUserName(userName);

        user.setUsername(userName);

        // merge with existing user
        try {
            ManagedUser existing = get(userName);
            if (user.getPassword() == null)
                user.setPassword(existing.getPassword());
            if (user.getAuthorities() == null || user.getAuthorities().isEmpty())
                user.setGrantedAuthorities(existing.getAuthorities());
        } catch (UsernameNotFoundException ex) {
            // that is OK, we create new
        }

        if (!user.isDefaultPassword()) {
            if (!checkPasswordLength(user.getPassword())) {
                throw new BadRequestException(msg.getSHORT_PASSWORD());
            }

            if (!checkPasswordCharacter(user.getPassword())) {
                throw new BadRequestException(msg.getINVALID_PASSWORD());
            }
        }

        user.setPassword(pwdEncode(user.getPassword()));

        logger.info("Saving " + user);

        completeAuthorities(user);
        userService.updateUser(user);
        return get(userName);
    }

    @RequestMapping(value = "/password", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    //change passwd
    public EnvelopeResponse save(@RequestBody PasswdChangeRequest user) {
        KapMessage msg = KapMsgPicker.getMsg();

        if (!"testing".equals(System.getProperty("spring.profiles.active"))) {
            throw new BadRequestException(msg.getUSER_EDIT_NOT_ALLOWED());
        }

        if (!isAdmin() && !StringUtils.equals(getPrincipal(), user.getUsername())) {
            throw new ForbiddenException(msg.getPERMISSION_DENIED());
        }
        checkUserName(user.getUsername());

        if (!checkPasswordLength(user.getNewPassword())) {
            throw new BadRequestException(msg.getSHORT_PASSWORD());
        }

        if (!checkPasswordCharacter(user.getNewPassword())) {
            throw new BadRequestException(msg.getINVALID_PASSWORD());
        }

        ManagedUser existing = get(user.getUsername());
        if (!isAdmin() && !pwdEncoder.matches(user.getPassword(), existing.getPassword())) {
            throw new BadRequestException(msg.getOLD_PASSWORD_WRONG());
        }

        existing.setPassword(pwdEncode(user.getNewPassword()));
        existing.setDefaultPassword(false);

        logger.info("update password for user " + user);

        completeAuthorities(existing);
        userService.updateUser(existing);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, get(user.getUsername()), "");
    }

    private String pwdEncode(String pwd) {
        if (bcryptPattern.matcher(pwd).matches())
            return pwd;

        return pwdEncoder.encode(pwd);
    }

    private void checkUserName(String userName) {
        KapMessage msg = KapMsgPicker.getMsg();

        if (userName == null || userName.isEmpty())
            throw new BadRequestException(msg.getEMPTY_USER_NAME());
    }

    private boolean checkPasswordLength(String password) {
        if (password == null || password.length() < 8)
            return false;
        return true;
    }

    private boolean checkPasswordCharacter(String password) {
        return passwordPattern.matcher(password).matches();
    }

    @RequestMapping(value = "/{userName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getUser(@PathVariable("userName") String userName) throws UsernameNotFoundException {
        KapMessage msg = KapMsgPicker.getMsg();

        if (!isAdmin() && !StringUtils.equals(getPrincipal(), userName)) {
            throw new ForbiddenException(msg.getPERMISSION_DENIED());
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, get(userName), "");
    }

    private ManagedUser get(String userName) {
        checkUserName(userName);

        UserDetails details = userService.loadUserByUsername(userName);
        if (details == null)
            return null;
        return (ManagedUser) details;
    }

    @RequestMapping(value = "/users", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse listAllUsers(
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "isCaseInsensitive", required = false) boolean isCaseInsensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        if (project == null) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }
        HashMap<String, Object> data = new HashMap<>();
        List<ManagedUser> result = listAllUsers();

        //for name fuzzy matching
        if (name != null) {
            for (Iterator<ManagedUser> it = result.iterator(); it.hasNext(); ) {
                ManagedUser user = it.next();
                String username;
                if (isCaseInsensitive) {
                    username = user.getUsername();
                } else {
                    username = user.getUsername().toUpperCase();
                }
                if (!username.contains(name.toUpperCase())) {
                    it.remove();
                }
            }
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (result.size() <= offset) {
            offset = result.size();
            limit = 0;
        }

        if ((result.size() - offset) < limit) {
            limit = result.size() - offset;
        }

        List<ManagedUser> subList = result.subList(offset, offset + limit);
        //LDAP users dose not have authorities
        for (ManagedUser user : subList) {
            userService.completeUserInfo(user);
        }
        data.put("users", subList);
        data.put("size", result.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/{userName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void delete(@PathVariable("userName") String userName) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        if (!"testing".equals(System.getProperty("spring.profiles.active"))) {
            throw new BadRequestException(msg.getUSER_EDIT_NOT_ALLOWED());
        }

        if (StringUtils.equals(getPrincipal(), userName)) {
            throw new ForbiddenException(msg.getSELF_DELETE_FORBIDDEN());
        }

        //delete user's project ACL
        accessService.revokeProjectPermission(userName, MetadataConstants.TYPE_USER);

        //delete user's table/row/column ACL
        ACLOperationUtil.delLowLevelACL(userName, MetadataConstants.TYPE_USER);

        checkUserName(userName);
        userService.deleteUser(userName);
    }

    private void completeAuthorities(ManagedUser managedUser) {
        List<SimpleGrantedAuthority> detailRoles = Lists.newArrayList(managedUser.getAuthorities());
        for (SimpleGrantedAuthority authority : detailRoles) {
            try {
                if (!userGroupService.exists(authority.getAuthority())) {
                    throw new RuntimeException("user's authority:" + authority.getAuthority() + " is not found in user group");
                }
            } catch (IOException e) {
                logger.error("Get user group error:" + e.getMessage());
            }
        }
        if (!detailRoles.contains(ALL_USERS_AUTH)) {
            detailRoles.add(ALL_USERS_AUTH);
        }
        managedUser.setGrantedAuthorities(detailRoles);
    }

    private String getPrincipal() {
        String userName = null;

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return null;
        }

        Object principal = authentication.getPrincipal();

        if (principal instanceof UserDetails) {
            userName = ((UserDetails) principal).getUsername();
        } else if (authentication.getDetails() instanceof UserDetails) {
            userName = ((UserDetails) authentication.getDetails()).getUsername();
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

    public List<ManagedUser> listAllUsers() throws IOException {
        return userService.listUsers();
    }

}
