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
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.Assert;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.kyligence.kap.rest.msg.KapMsgPicker;

public class KapAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(KapAuthenticationProvider.class);

    private final static com.google.common.cache.Cache<String, Authentication> userCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, Authentication>() {
                @Override
                public void onRemoval(RemovalNotification<String, Authentication> notification) {
                    KapAuthenticationProvider.logger.debug("User cache {} is removed due to {}", notification.getKey(),
                            notification.getCause());
                }
            }).build();

    @Autowired
    @Qualifier("userService")
    UserService userService;

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

        md.reset();
        byte[] hashKey = md.digest((authentication.getName() + authentication.getCredentials()).getBytes());
        String userKey = Arrays.toString(hashKey);

        if (userService.isEvictCacheFlag()) {
            userCache.invalidateAll();
            userService.setEvictCacheFlag(false);
        }
        
        Authentication authed = userCache.getIfPresent(userKey);

        ManagedUser managedUser = null;
        String userName = null;

        if (null != authed) {
            SecurityContextHolder.getContext().setAuthentication(authed);
        } else {
            try {
                if (authentication instanceof UsernamePasswordAuthenticationToken)
                    userName = (String) authentication.getPrincipal();

                if (userName != null && userService.userExists(userName)) {
                    managedUser = (ManagedUser) userService.loadUserByUsername(userName);
                    Preconditions.checkNotNull(managedUser);
                }

                if (managedUser != null && managedUser.isLocked()) {
                    long lockedTime = managedUser.getLockedTime();
                    long timeDiff = System.currentTimeMillis() - lockedTime;

                    if (timeDiff > 30000) {
                        managedUser.setLocked(false);
                        userService.updateUser(managedUser);
                    } else {
                        int leftSeconds = (30 - timeDiff / 1000) <= 0 ? 1 : (int) (30 - timeDiff / 1000);
                        String msg = String.format(KapMsgPicker.getMsg().getUSER_LOCK(), userName, leftSeconds);
                        throw new LockedException(msg, new Throwable(userName));
                    }
                }

                authed = authenticationProvider.authenticate(authentication);

                //update the user because LDAP authed may bring new authorities
                ManagedUser user;

                if (authed.getDetails() == null) {
                    //authed.setAuthenticated(false);
                    throw new UsernameNotFoundException(
                            "User not found in LDAP, check whether he/she has been added to the groups.");
                }

                if (authed.getDetails() instanceof UserDetails) {
                    UserDetails details = (UserDetails) authed.getDetails();
                    user = new ManagedUser(details.getUsername(), details.getPassword(), false,
                            details.getAuthorities());
                } else {
                    user = new ManagedUser(authentication.getName(), "skippped-ldap", false, authed.getAuthorities());
                }

                Assert.notNull(user, "The UserDetail is null.");

                logger.debug("User {} authorities : {}", user.getUsername(), user.getAuthorities());

                if (!userService.userExists(user.getUsername())) {
                    userService.createUser(user);
                } else {
                    userService.updateUser(user);
                }

                userCache.put(userKey, authed);
            } catch (AuthenticationException e) {
                if (userName != null && managedUser != null) {
                    managedUser.increaseWrongTime();
                    userService.updateUser(managedUser);
                }
                logger.error("Failed to auth user: " + authentication.getName(), e);
                throw new BadCredentialsException(KapMsgPicker.getMsg().getUSER_AUTHFAILED(), e);
            }

            logger.debug("Authenticated user " + authed.toString());

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
