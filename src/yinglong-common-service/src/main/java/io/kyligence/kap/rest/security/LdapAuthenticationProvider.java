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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.Assert;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import io.kyligence.kap.rest.service.LdapUserService;

/**
 * A wrapper class for the authentication provider; Will do something more for Kylin.
 */
public class LdapAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(LdapAuthenticationProvider.class);

    @Autowired
    @Qualifier("userService")
    LdapUserService ldapUserService;

    private final AuthenticationProvider authenticationProvider;

    private final HashFunction hf;

    public LdapAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        Assert.notNull(authenticationProvider, "The embedded authenticationProvider should not be null.");
        this.authenticationProvider = authenticationProvider;
        this.hf = Hashing.murmur3_128();
    }

    @Override
    public Authentication authenticate(Authentication authentication) {
        Authentication auth = null;
        try {
            auth = authenticationProvider.authenticate(authentication);
        } catch (BadCredentialsException e) {
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg(), e);
        } catch (AuthenticationException ae) {
            logger.error("Failed to auth user: {}", authentication.getName(), ae);
            throw ae;
        } catch (IncorrectResultSizeDataAccessException e) {
            logger.error("Ldap username {} is not unique", authentication.getName());
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg(), e);
        }

        if (auth.getDetails() == null) {
            throw new UsernameNotFoundException(
                    "User not found in LDAP, check whether he/she has been added to the groups.");
        }

        String userName = auth.getDetails() instanceof UserDetails ? ((UserDetails) auth.getDetails()).getUsername()
                : authentication.getName();

        ldapUserService.onUserAuthenticated(userName);
        logger.debug("Authenticated userName: {}", userName);
        return auth;
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authenticationProvider.supports(authentication);
    }
}
