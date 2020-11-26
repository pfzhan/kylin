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
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_PASSWORD;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_USER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PASSWORD;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.SHORT_PASSWORD;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_NOT_EXIST;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.util.PasswordEncodeFactory;
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
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
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
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.config.initialize.AfterMetadataReadyEvent;
import io.kyligence.kap.rest.request.PasswordChangeRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.util.CreateAdminUserUtils;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/user", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NUserController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NUserController.class);

    private static final String PROFILE_DEFAULT = "testing";

    private static final String PROFILE_CUSTOM = "custom";

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
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    private Environment env;

    private static final Pattern passwordPattern = Pattern
            .compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");
    private static final Pattern bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
    private static final Pattern base64Pattern = Pattern
            .compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");

    private static PasswordEncoder pwdEncoder = PasswordEncodeFactory.newUserPasswordEncoder();

    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    @EventListener(AfterMetadataReadyEvent.class)
    public void init() throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.isUTEnv()) {
            return;
        }
        CreateAdminUserUtils.createAllAdmins(userService, env);
    }

    @ApiOperation(value = "createUser", notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PostMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> createUser(@RequestBody ManagedUser user) throws IOException {
        val username = user.getUsername();
        val password = pwdBase64Decode(user.getPassword());
        user.setPassword(password);

        checkUsername(username);
        checkPasswordLength(password);
        checkPasswordCharacter(password);
        checkUserGroupNotEmpty(user.getAuthorities());
        checkUserGroupExists(user.getAuthorities());
        checkUserGroupNotDuplicated(user.getAuthorities());

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

    @ApiOperation(value = "updateUser", notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PutMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> updateUser(@RequestBody ManagedUser user) throws IOException {
        val msg = MsgPicker.getMsg();
        checkProfile();

        if (StringUtils.equals(getPrincipal(), user.getUsername()) && user.isDisabled()) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getSELF_DISABLE_FORBIDDEN());
        }
        val username = user.getUsername();
        checkUsername(username);

        // merge with existing user
        val existing = getManagedUser(username);
        if (existing == null) {
            throw new KylinException(USER_NOT_EXIST, String.format(msg.getUSER_NOT_FOUND(), username));
        }
        if (StringUtils.isEmpty(user.getPassword()))
            user.setPassword(existing.getPassword());
        if (user.getAuthorities() == null || user.getAuthorities().isEmpty())
            user.setGrantedAuthorities(existing.getAuthorities());

        user.setPassword(pwdBase64Decode(user.getPassword()));

        if (!user.isDefaultPassword()) {
            checkPasswordLength(user.getPassword());
            checkPasswordCharacter(user.getPassword());
        }

        user.setPassword(pwdEncode(user.getPassword()));
        checkUserGroupExists(user.getAuthorities());
        checkUserGroupNotDuplicated(user.getAuthorities());
        completeAuthorities(user);

        boolean noAdminRight = user.getAuthorities().contains(new SimpleGrantedAuthority(ROLE_ADMIN));
        accessService.checkDefaultAdmin(user.getUsername(), noAdminRight);

        logger.info("Saving user {}", user);
        userService.updateUser(user);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @DeleteMapping(value = "/{uuid:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> deleteByUUID(@PathVariable("uuid") String userUUID) {
        val msg = MsgPicker.getMsg();

        checkProfile();

        ManagedUser toBeDeleteUser = null;
        try {
            toBeDeleteUser = userService.listUsers().stream().filter(user -> userUUID.equalsIgnoreCase(user.getUuid()))
                    .findFirst().orElse(null);
        } catch (IOException e) {
            logger.error("List all users is failed!", e);
        }

        if (Objects.isNull(toBeDeleteUser)) {
            throw new KylinException(USER_NOT_EXIST, String.format(msg.getUSER_NOT_EXIST(), userUUID));
        }

        if (StringUtils.equals(getPrincipal(), toBeDeleteUser.getUsername())) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getSELF_DELETE_FORBIDDEN());
        }
        accessService.checkDefaultAdmin(toBeDeleteUser.getUsername(), false);
        //delete user's project ACL
        accessService.revokeProjectPermission(toBeDeleteUser.getUsername(), MetadataConstants.TYPE_USER);
        aclTCRService.revokeAclTCR(toBeDeleteUser.getUsername(), true);
        userService.deleteUser(toBeDeleteUser.getUsername());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    //@DeleteMapping(value = "/{username:.+}")
    //@ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delete(@PathVariable("username") String username) {
        val msg = MsgPicker.getMsg();

        checkProfile();
        checkUsername(username);

        ManagedUser managedUser = null;
        try {
            managedUser = getManagedUser(username);
        } catch (UsernameNotFoundException e) {
            logger.warn("Delete user failed, user {} not found.", username);
        }
        if (Objects.isNull(managedUser)) {
            throw new KylinException(USER_NOT_EXIST, String.format(msg.getUSER_NOT_FOUND(), username));
        }

        if (StringUtils.equals(getPrincipal(), username)) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getSELF_DELETE_FORBIDDEN());
        }
        accessService.checkDefaultAdmin(username, false);
        //delete user's project ACL
        accessService.revokeProjectPermission(username, MetadataConstants.TYPE_USER);
        aclTCRService.revokeAclTCR(username, true);
        userService.deleteUser(username);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "listAllUsers", notes = "Update Param: is_case_sensitive, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<ManagedUser>>> listAllUsers(
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> usersByFuzzyMatching = userService.getManagedUsersByFuzzMatching(nameSeg, isCaseSensitive);
        List<ManagedUser> subList = PagingUtil.cutPage(usersByFuzzyMatching, pageOffset, pageSize);
        //LDAP users dose not have authorities
        for (ManagedUser u : subList) {
            userService.completeUserInfo(u);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(subList, usersByFuzzyMatching, pageOffset, pageSize), "");
    }

    @PutMapping(value = "/password")
    @ResponseBody
    //change passwd
    public EnvelopeResponse<String> updateUserPassword(@RequestBody PasswordChangeRequest user) {
        val msg = MsgPicker.getMsg();
        val username = user.getUsername();

        if (!isAdmin() && !StringUtils.equals(getPrincipal(), username)) {
            throw new KylinException(PERMISSION_DENIED, msg.getPERMISSION_DENIED());
        }
        accessService.checkDefaultAdmin(username, true);
        val oldPassword = pwdBase64Decode(user.getPassword());
        val newPassword = pwdBase64Decode(user.getNewPassword());

        checkUsername(username);

        checkPasswordLength(newPassword);

        checkPasswordCharacter(newPassword);

        ManagedUser existingUser = getManagedUser(username);
        if (existingUser == null) {
            throw new KylinException(USER_NOT_EXIST, String.format(msg.getUSER_NOT_FOUND(), username));
        }
        val actualOldPassword = existingUser.getPassword();

        // when reset oneself's password (includes ADMIN users), check old password
        if (StringUtils.equals(getPrincipal(), username)) {
            if (!pwdEncoder.matches(oldPassword, actualOldPassword)) {
                throw new KylinException(FAILED_UPDATE_PASSWORD, msg.getOLD_PASSWORD_WRONG());
            }
        }

        if (newPassword.equals(oldPassword)) {
            throw new KylinException(FAILED_UPDATE_PASSWORD, msg.getNEW_PASSWORD_SAME_AS_OLD());
        }

        existingUser.setPassword(pwdEncode(newPassword));
        existingUser.setDefaultPassword(false);

        logger.info("update password for user {}", user);

        existingUser.clearAuthenticateFailedRecord();

        completeAuthorities(existingUser);
        userService.updateUser(existingUser);

        // update authentication
        if (StringUtils.equals(getPrincipal(), user.getUsername())) {
            UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(existingUser,
                    newPassword, existingUser.getAuthorities());
            token.setDetails(SecurityContextHolder.getContext().getAuthentication().getDetails());
            SecurityContextHolder.getContext().setAuthentication(token);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/authentication", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticate() {
        checkLicense();
        EnvelopeResponse<UserDetails> response = authenticatedUser();
        logger.debug("User login: {}", response.getData());
        return response;
    }

    @PostMapping(value = "/update_user")
    @ResponseBody
    public EnvelopeResponse<UserDetails> updateUserWithoutAuth(@RequestBody ManagedUser user) {
        userService.updateUser(user);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    private void checkLicense() {
        if (!KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            val info = licenseInfoService.extractLicenseInfo();
            licenseInfoService.verifyLicense(info);
        }
    }

    @GetMapping(value = "/authentication", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
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
            throw new KylinException(INVALID_PASSWORD, msg.getINVALID_PASSWORD());
        }
    }

    private void checkProfile() {
        val msg = MsgPicker.getMsg();
        if (!env.acceptsProfiles(PROFILE_DEFAULT, PROFILE_CUSTOM)) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getUSER_EDIT_NOT_ALLOWED());
        }
    }

    private void checkPasswordLength(String password) {
        val msg = MsgPicker.getMsg();
        if (password == null || password.length() < 8)
            throw new KylinException(SHORT_PASSWORD, msg.getSHORT_PASSWORD());
    }

    private void checkUsername(String username) {
        if (env.acceptsProfiles(PROFILE_CUSTOM))
            return;

        val msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(username)) {
            throw new KylinException(EMPTY_USER_NAME, msg.getEMPTY_USER_NAME());
        }
        if (username.startsWith(".")) {
            throw new KylinException(EMPTY_USER_NAME, msg.getINVALID_NAME_START_WITH_DOT());
        }
        if (!username.equals(username.trim())) {
            throw new KylinException(EMPTY_USER_NAME, msg.getINVALID_NAME_START_OR_END_WITH_BLANK());
        }
        if (Pattern.compile("[^\\x00-\\xff]").matcher(username).find()) {
            throw new KylinException(EMPTY_USER_NAME, msg.getINVALID_NAME_CONTAINS_OTHER_CHARACTER());
        }
        if (Pattern.compile("[\\\\/:*?\"<>|]").matcher(username).find()) {
            throw new KylinException(EMPTY_USER_NAME, msg.getINVALID_NAME_CONTAINS_INLEGAL_CHARACTER());
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

    public ManagedUser getManagedUser(String userName) {
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

    /**
     * decode base64 password
     * @param password
     * @return base64 decode password if password is base64 encode else password
     */
    private String pwdBase64Decode(String password) {
        boolean isMatch = base64Pattern.matcher(password).matches();
        if (isMatch) {
            return new String(Base64.decodeBase64(password));
        }
        return password;
    }

    private void checkUserGroupNotEmpty(List<SimpleGrantedAuthority> groups) {
        boolean hasEmptyGroup = CollectionUtils.isEmpty(groups)
                || groups.stream().map(SimpleGrantedAuthority::getAuthority).anyMatch(StringUtils::isBlank);
        if (hasEmptyGroup) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getEMPTY_GROUP_NAME());
        }
    }

    private void checkUserGroupExists(List<SimpleGrantedAuthority> groups) throws IOException {
        for (SimpleGrantedAuthority group : groups) {
            if (!userGroupService.exists(group.getAuthority())) {
                throw new KylinException(INVALID_PARAMETER,
                        String.format(MsgPicker.getMsg().getOPERATION_FAILED_BY_GROUP_NOT_EXIST(), group));
            }
        }
    }

    private void checkUserGroupNotDuplicated(List<SimpleGrantedAuthority> groups) {
        List<String> authorities = groups.stream().map(SimpleGrantedAuthority::getAuthority).collect(Collectors.toList());
        if (authorities.size() != Sets.newHashSet(authorities).size()) {
            throw new KylinException(INVALID_PARAMETER, "Values in authorities can't be duplicated.");
        }
    }

}
