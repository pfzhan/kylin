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

import com.google.common.collect.Lists;
import io.kyligence.kap.rest.config.AppInitializer;
import io.kyligence.kap.rest.request.PasswordChangeRequest;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

@Controller
@RequestMapping(value = "/user")
public class NUserController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NUserController.class);

    private static final String PROFILE = "testing";

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    private Environment env;

    private String activeProfile = PROFILE;

    private static final Message msg = MsgPicker.getMsg();

    private static Pattern passwordPattern = Pattern
            .compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");
    private static Pattern bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
    private static BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    @EventListener(AppInitializer.AppInitializedEvent.class)
    public void init() throws IOException {
        List<ManagedUser> all = userService.listUsers();
        activeProfile = env.getActiveProfiles()[0];
        logger.info("All " + all.size() + " users");
        if (all.isEmpty() && PROFILE.equals(activeProfile)) {
            createUser(new ManagedUser("ADMIN", "KYLIN", true, Constant.ROLE_ADMIN, Constant.GROUP_ALL_USERS));
            createUser(new ManagedUser("ANALYST", "ANALYST", true, Constant.GROUP_ALL_USERS));
            createUser(new ManagedUser("MODELER", "MODELER", true, Constant.GROUP_ALL_USERS));
        }

    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse createUser(@RequestBody ManagedUser user) {
        checkProfile();
        user.setUuid(UUID.randomUUID().toString());
        user.setPassword(pwdEncode(user.getPassword()));
        logger.info("Creating " + user);
        completeAuthorities(user);
        userService.createUser(user);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse updateUser(@RequestBody ManagedUser user) {

        checkProfile();

        if (StringUtils.equals(getPrincipal(), user.getUsername()) && user.isDisabled()) {
            throw new ForbiddenException(msg.getSELF_DISABLE_FORBIDDEN());
        }
        val username = user.getUsername();
        checkUsername(username);

        // merge with existing user
        val existing = getManagedUser(username);
        if (existing == null) {
            throw new BadRequestException(String.format(msg.getUSER_NOT_FOUND(), username));
        }
        if (StringUtils.isEmpty(user.getPassword()))
            user.setPassword(existing.getPassword());
        if (user.getAuthorities() == null || user.getAuthorities().isEmpty())
            user.setGrantedAuthorities(existing.getAuthorities());

        if (!user.isDefaultPassword()) {
            checkPasswordLength(user.getPassword());
            checkPasswordCharacter(user.getPassword());
        }

        user.setPassword(pwdEncode(user.getPassword()));

        logger.info("Saving " + user);

        completeAuthorities(user);
        userService.updateUser(user);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/{username}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse delete(@PathVariable("username") String username) throws IOException {

        checkProfile();
        checkUsername(username);
        if (StringUtils.equals(getPrincipal(), username)) {
            throw new ForbiddenException(msg.getSELF_DELETE_FORBIDDEN());
        }
        //delete user's project ACL
        accessService.revokeProjectPermission(username, MetadataConstants.TYPE_USER);
        userService.deleteUser(username);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listAllUsers(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        if (project == null) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }
        HashMap<String, Object> data = new HashMap<>();
        List<ManagedUser> usersByFuzzyMatching = userService.getManagedUsersByFuzzMatching(nameSeg, isCaseSensitive);
        List<ManagedUser> subList = PagingUtil.cutPage(usersByFuzzyMatching, pageOffset, pageSize);
        //LDAP users dose not have authorities
        for (ManagedUser u : subList) {
            userService.completeUserInfo(u);
        }
        data.put("users", subList);
        data.put("size", usersByFuzzyMatching.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/password", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    //change passwd
    public EnvelopeResponse updateUserPassword(@RequestBody PasswordChangeRequest user) {

        if (!isAdmin() && !StringUtils.equals(getPrincipal(), user.getUsername())) {
            throw new ForbiddenException(msg.getPERMISSION_DENIED());
        }
        val username = user.getUsername();
        checkUsername(username);

        checkPasswordLength(user.getNewPassword());

        checkPasswordCharacter(user.getNewPassword());

        ManagedUser existingUser = getManagedUser(username);
        if (existingUser == null) {
            throw new BadRequestException(String.format(msg.getUSER_NOT_FOUND(), username));
        }

        if (!isAdmin() && !pwdEncoder.matches(user.getPassword(), existingUser.getPassword())) {
            throw new BadRequestException(msg.getOLD_PASSWORD_WRONG());
        }

        existingUser.setPassword(pwdEncode(user.getNewPassword()));
        existingUser.setDefaultPassword(false);

        logger.info("update password for user " + user);

        completeAuthorities(existingUser);
        userService.updateUser(existingUser);

        // update authentication
        if (StringUtils.equals(getPrincipal(), user.getUsername())) {
            UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(existingUser,
                    user.getNewPassword(), existingUser.getAuthorities());
            token.setDetails(SecurityContextHolder.getContext().getAuthentication().getDetails());
            SecurityContextHolder.getContext().setAuthentication(token);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/authentication", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticate() {
        EnvelopeResponse response = authenticatedUser();
        logger.debug("User login: {}", response.data);
        return response;
    }

    @RequestMapping(value = "/authentication", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticatedUser() {
        Message msg = MsgPicker.getMsg();

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails data = null;

        if (authentication == null) {
            logger.debug("authentication is null.");
            throw new UnauthorizedException(msg.getAUTH_INFO_NOT_FOUND());
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            data = (UserDetails) authentication.getPrincipal();
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
        }

        if (authentication.getDetails() instanceof UserDetails) {
            data = (UserDetails) authentication.getDetails();
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
        }

        throw new UnauthorizedException(msg.getAUTH_INFO_NOT_FOUND());
    }

    private void checkPasswordCharacter(String password) {
        if (!passwordPattern.matcher(password).matches()) {
            throw new BadRequestException(msg.getINVALID_PASSWORD());
        }
    }

    private void checkProfile() {
        if (!PROFILE.equals(activeProfile)) {
            throw new BadRequestException(msg.getUSER_EDIT_NOT_ALLOWED());
        }
    }

    private void checkPasswordLength(String password) {
        if (password == null || password.length() < 8)
            throw new BadRequestException(msg.getSHORT_PASSWORD());
    }

    private void checkUsername(String username) {
        if (StringUtils.isEmpty(username))
            throw new BadRequestException(msg.getEMPTY_USER_NAME());
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

    private ManagedUser getManagedUser(String userName) {
        UserDetails details = userService.loadUserByUsername(userName);
        if (details == null)
            return null;
        return (ManagedUser) details;
    }

    private void completeAuthorities(ManagedUser managedUser) {
        List<SimpleGrantedAuthority> detailRoles = Lists.newArrayList(managedUser.getAuthorities());
        if (!detailRoles.contains(ALL_USERS_AUTH)) {
            detailRoles.add(ALL_USERS_AUTH);
        }
        managedUser.setGrantedAuthorities(detailRoles);
    }

    private String pwdEncode(String pwd) {
        if (bcryptPattern.matcher(pwd).matches())
            return pwd;

        return pwdEncoder.encode(pwd);
    }
}
