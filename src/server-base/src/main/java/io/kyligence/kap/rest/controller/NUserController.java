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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.LicenseInfoService;
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
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.config.initialize.AppInitializedEvent;
import io.kyligence.kap.rest.request.PasswordChangeRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/user")
public class NUserController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NUserController.class);

    private static final String PROFILE = "testing";

    private static final Pattern sidPattern = Pattern.compile("^[a-zA-Z0-9_ ]*$");
    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    private LicenseInfoService licenseInfoService;

    @Autowired
    private Environment env;

    private static final Pattern passwordPattern = Pattern
            .compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");
    private static final Pattern bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
    private static BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    @EventListener(AppInitializedEvent.class)
    public void init() throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        if ("query".equals(config.getServerMode())) {
            return;
        }
        List<ManagedUser> all = userService.listUsers();
        logger.info("All {} users", all.size());
        if (all.isEmpty() && env.acceptsProfiles(PROFILE)) {
            createAdminUser(new ManagedUser("ADMIN", "KYLIN", true, Constant.ROLE_ADMIN, Constant.GROUP_ALL_USERS));
            createAdminUser(new ManagedUser("ANALYST", "ANALYST", true, Constant.GROUP_ALL_USERS));
            createAdminUser(new ManagedUser("MODELER", "MODELER", true, Constant.GROUP_ALL_USERS));
        }

    }

    @ApiOperation(value = "createUser (update)", notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> createUser(@RequestBody ManagedUser user) {
        val username = user.getUsername();
        val password = user.getPassword();

        checkUsername(username);
        checkPasswordLength(password);
        checkPasswordCharacter(password);

        return createAdminUser(user);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> createAdminUser(@RequestBody ManagedUser user) {
        checkProfile();
        user.setUuid(UUID.randomUUID().toString());
        user.setPassword(pwdEncode(user.getPassword()));
        logger.info("Creating user: {}", user);
        completeAuthorities(user);
        userService.createUser(user);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateUser (update)", notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PutMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> updateUser(@RequestBody ManagedUser user) {
        val msg = MsgPicker.getMsg();
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

        logger.info("Saving user {}", user);

        completeAuthorities(user);
        userService.updateUser(user);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @DeleteMapping(value = "/{username:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delete(@PathVariable("username") String username) {
        val msg = MsgPicker.getMsg();

        checkProfile();
        checkUsername(username);
        if (StringUtils.equals(getPrincipal(), username)) {
            throw new ForbiddenException(msg.getSELF_DELETE_FORBIDDEN());
        }
        //delete user's project ACL
        accessService.revokeProjectPermission(username, MetadataConstants.TYPE_USER);
        aclTCRService.revokeAclTCR(username, true);
        userService.deleteUser(username);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "listAllUsers (update)", notes = "Update Param: is_case_sensitive, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<ManagedUser>>> listAllUsers(
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        if (project == null) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }

        List<ManagedUser> usersByFuzzyMatching = userService.getManagedUsersByFuzzMatching(nameSeg, isCaseSensitive);
        List<ManagedUser> subList = PagingUtil.cutPage(usersByFuzzyMatching, pageOffset, pageSize);
        //LDAP users dose not have authorities
        for (ManagedUser u : subList) {
            userService.completeUserInfo(u);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(subList, usersByFuzzyMatching), "");
    }

    @PutMapping(value = "/password", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    //change passwd
    public EnvelopeResponse<String> updateUserPassword(@RequestBody PasswordChangeRequest user) {
        val msg = MsgPicker.getMsg();
        val username = user.getUsername();

        if (!isAdmin() && !StringUtils.equals(getPrincipal(), username)) {
            throw new ForbiddenException(msg.getPERMISSION_DENIED());
        }

        val oldPassword = user.getPassword();
        val newPassword = user.getNewPassword();

        checkUsername(username);

        checkPasswordLength(user.getNewPassword());

        checkPasswordCharacter(user.getNewPassword());

        ManagedUser existingUser = getManagedUser(username);
        if (existingUser == null) {
            throw new BadRequestException(String.format(msg.getUSER_NOT_FOUND(), username));
        }
        val actualOldPassword = existingUser.getPassword();

        if (!isAdmin() && !pwdEncoder.matches(oldPassword, actualOldPassword)) {
            throw new BadRequestException(msg.getOLD_PASSWORD_WRONG());
        }

        if (newPassword.equals(oldPassword)) {
            throw new BadRequestException(msg.getNEW_PASSWORD_SAME_AS_OLD());
        }

        existingUser.setPassword(pwdEncode(newPassword));
        existingUser.setDefaultPassword(false);

        logger.info("update password for user {}", user);

        completeAuthorities(existingUser);
        userService.updateUser(existingUser);

        // update authentication
        if (StringUtils.equals(getPrincipal(), user.getUsername())) {
            UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(existingUser,
                    user.getNewPassword(), existingUser.getAuthorities());
            token.setDetails(SecurityContextHolder.getContext().getAuthentication().getDetails());
            SecurityContextHolder.getContext().setAuthentication(token);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/authentication", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticate() {
        checkLicense();
        EnvelopeResponse<UserDetails> response = authenticatedUser();
        logger.debug("User login: {}", response.getData());
        return response;
    }

    private void checkLicense() {
        if (!KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            val info = licenseInfoService.extractLicenseInfo();
            licenseInfoService.verifyLicense(info);
        }
    }

    @GetMapping(value = "/authentication", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticatedUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails data = null;
        val msg = MsgPicker.getMsg();
        if (authentication == null) {
            throw new UnauthorizedException(msg.getAUTH_INFO_NOT_FOUND());
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            data = (UserDetails) authentication.getPrincipal();
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
        }

        if (authentication.getDetails() instanceof UserDetails) {
            data = (UserDetails) authentication.getDetails();
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
        }

        throw new UnauthorizedException(msg.getAUTH_INFO_NOT_FOUND());
    }

    private void checkPasswordCharacter(String password) {
        val msg = MsgPicker.getMsg();
        if (!passwordPattern.matcher(password).matches()) {
            throw new BadRequestException(msg.getINVALID_PASSWORD());
        }
    }

    private void checkProfile() {
        val msg = MsgPicker.getMsg();
        if (!env.acceptsProfiles(PROFILE)) {
            throw new BadRequestException(msg.getUSER_EDIT_NOT_ALLOWED());
        }
    }

    private void checkPasswordLength(String password) {
        val msg = MsgPicker.getMsg();
        if (password == null || password.length() < 8)
            throw new BadRequestException(msg.getSHORT_PASSWORD());
    }

    private void checkUsername(String username) {
        val msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(username)) {
            throw new BadRequestException(msg.getEMPTY_USER_NAME());
        }
        if (!sidPattern.matcher(username).matches()) {
            throw new BadRequestException(MsgPicker.getMsg().getINVALID_SID());
        }
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

    ManagedUser getManagedUser(String userName) {
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
