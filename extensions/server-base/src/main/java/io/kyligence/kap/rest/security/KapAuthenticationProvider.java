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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.Assert;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class KapAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(KapAuthenticationProvider.class);

    @Autowired
    private KapAuthenticationManager kapAuthenticationManager;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    private CacheManager cacheManager;

    //Embedded authentication provider
    private AuthenticationProvider authenticationProvider;

    MessageDigest md = null;

    public KapAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        super();
        Assert.notNull(authenticationProvider, "The embedded authenticationProvider should not be null.");
        this.authenticationProvider = authenticationProvider;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to init Message Digest ", e);
        }
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        Authentication authed = null;
        Cache userCache = cacheManager.getCache("UserCache");
        md.reset();
        byte[] hashKey = md.digest((authentication.getName() + authentication.getCredentials()).getBytes());
        String userKey = Arrays.toString(hashKey);
        String userName = null;

        Element authedUser = userCache.get(userKey);
        if (null != authedUser) {
            authed = (Authentication) authedUser.getObjectValue();
            SecurityContextHolder.getContext().setAuthentication(authed);
        } else {
            try {
                if (authentication instanceof UsernamePasswordAuthenticationToken)
                    userName = (String) authentication.getPrincipal();

                if (kapAuthenticationManager.isUserLocked(userName)) {
                    long lockedTime = kapAuthenticationManager.getLockedTime(userName);
                    long timeDiff = System.currentTimeMillis() - lockedTime;

                    if (timeDiff > 30000) {
                        kapAuthenticationManager.unlockUser(userName);
                    } else {
                        int leftSeconds = (30 - timeDiff / 1000) <= 0 ? 1 : (int) (30 - timeDiff / 1000);
                        String msg = "User " + userName + " is locked, please wait for " + leftSeconds + " seconds.";
                        throw new LockedException(msg, new Throwable(userName));
                    }
                }

                authed = authenticationProvider.authenticate(authentication);
                userCache.put(new Element(userKey, authed));
            } catch (AuthenticationException e) {
                if (userName != null) {
                    kapAuthenticationManager.increaseWrongTime(userName);
                }
                logger.error("Failed to auth user: " + authentication.getName(), e);
                throw e;
            }

            logger.debug("Authenticated user " + authed.toString());

            UserDetails user;

            if (authed.getDetails() == null) {
                //authed.setAuthenticated(false);
                throw new UsernameNotFoundException(
                        "User not found in LDAP, check whether he/she has been added to the groups.");
            }

            if (authed.getDetails() instanceof UserDetails) {
                user = (UserDetails) authed.getDetails();
            } else {
                user = new User(authentication.getName(), "skippped-ldap", authed.getAuthorities());
            }
            Assert.notNull(user, "The UserDetail is null.");

            logger.debug("User authorities :" + user.getAuthorities());
            if (!userService.userExists(user.getUsername())) {
                userService.createUser(user);
            } else {
                userService.updateUser(user);
            }
        }

        return authed;
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authenticationProvider.supports(authentication);
    }

    public AuthenticationProvider getAuthenticationProvider() {
        return authenticationProvider;
    }

    public void setAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

}
