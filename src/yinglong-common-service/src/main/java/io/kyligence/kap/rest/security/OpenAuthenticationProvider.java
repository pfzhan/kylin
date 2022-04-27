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

package io.kyligence.kap.rest.security;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import io.kyligence.kap.common.annotation.ThirdPartyDependencies;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.service.NUserGroupService;

@ThirdPartyDependencies({
        @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager",
                classes = {"StaticAuthenticationProvider"})
})
public abstract class OpenAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(OpenAuthenticationProvider.class);

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    NUserGroupService userGroupService;

    public UserService getUserService() {
        return userService;
    }

    public NUserGroupService getUserGroupService() {
        return userGroupService;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        if (!authenticateImpl(authentication)) {
            logger.error("Failed to auth user: {}", authentication.getPrincipal());
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg());
        }
        ManagedUser user;
        try {
            user = (ManagedUser) getUserService().loadUserByUsername((String) authentication.getPrincipal());
        } catch (Exception e) {
            String userName = (String) authentication.getPrincipal();
            String password = (String) authentication.getCredentials();
            user = new ManagedUser(userName, password, true, Constant.GROUP_ALL_USERS);
        }
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(user, authentication.getCredentials(),
                user.getAuthorities());
        return token;
    }

    /**
     * The method verifys that whether the user being logged in is legal or not.
     * @param authentication : this object contains two key attributes
     *               principal, it is a String type and it represents the username uploaded from the page.
     *               credentials, it is a String type and it represents the unencrypted password uploaded from the page.
     * @return Whether you allow this user to log in
     */
    public abstract boolean authenticateImpl(Authentication authentication);

    @Override
    public final boolean supports(Class<?> aClass) {
        return true;
    }
}
