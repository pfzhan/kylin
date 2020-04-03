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
package io.kyligence.kap.rest.util;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.user.ManagedUser;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.exceptions.KylinException;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.UserService;
import org.springframework.core.env.Environment;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.RequestBody;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

@Slf4j
public class CreateAdminUserUtils {

    private static BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    private static final Pattern bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");

    private static final String PROFILE_DEFAULT = "testing";

    private static final String PROFILE_CUSTOM = "custom";

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public static EnvelopeResponse<String> createAdminUser(@RequestBody ManagedUser user, UserService userService, Environment env) {
        checkProfile(env);
        user.setUuid(UUID.randomUUID().toString());
        user.setPassword(pwdEncode(user.getPassword()));
        log.info("Creating user: {}", user);
        completeAuthorities(user);
        userService.createUser(user);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    public static String pwdEncode(String pwd) {
        if (bcryptPattern.matcher(pwd).matches())
            return pwd;

        return pwdEncoder.encode(pwd);
    }

    public static void checkProfile(Environment env) {
        val msg = MsgPicker.getMsg();
        if (!env.acceptsProfiles(PROFILE_DEFAULT, PROFILE_CUSTOM)) {
            throw new KylinException("KE-1006", msg.getUSER_EDIT_NOT_ALLOWED());
        }
    }

    public static void completeAuthorities(ManagedUser managedUser) {
        List<SimpleGrantedAuthority> detailRoles = Lists.newArrayList(managedUser.getAuthorities());
        if (!detailRoles.contains(ALL_USERS_AUTH)) {
            detailRoles.add(ALL_USERS_AUTH);
        }
        managedUser.setGrantedAuthorities(detailRoles);
    }

    public static void createAllAdmins(UserService userService, Environment env) throws IOException {
        List<ManagedUser> all = userService.listUsers();
        log.info("All {} users", all.size());
        if (all.isEmpty() && env.acceptsProfiles(PROFILE_DEFAULT)) {
            createAdminUser(new ManagedUser("ADMIN", "KYLIN", true, ROLE_ADMIN, Constant.GROUP_ALL_USERS), userService, env);
            createAdminUser(new ManagedUser("ANALYST", "ANALYST", true, Constant.GROUP_ALL_USERS), userService, env);
            createAdminUser(new ManagedUser("MODELER", "MODELER", true, Constant.GROUP_ALL_USERS), userService, env);
        }

    }
}
