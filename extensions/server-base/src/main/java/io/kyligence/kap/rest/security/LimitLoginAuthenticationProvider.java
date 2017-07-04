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
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.kyligence.kap.rest.msg.KapMsgPicker;

public class LimitLoginAuthenticationProvider extends DaoAuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(LimitLoginAuthenticationProvider.class);

    private final static com.google.common.cache.Cache<String, Authentication> userCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, Authentication>() {
                @Override
                public void onRemoval(RemovalNotification<String, Authentication> notification) {
                    LimitLoginAuthenticationProvider.logger.debug("User cache {} is removed due to {}",
                            notification.getKey(), notification.getCause());
                }
            }).build();

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {

        MessageDigest md = null;

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to init Message Digest ", e);
        }

        md.reset();
        byte[] hashKey = md.digest((authentication.getName() + authentication.getCredentials()).getBytes());
        String userKey = Arrays.toString(hashKey);

        if (userService.isEvictCacheFlag()) {
            userCache.invalidateAll();
            userService.setEvictCacheFlag(false);
        }
        Authentication auth = userCache.getIfPresent(userKey);

        ManagedUser managedUser = null;
        String userName = null;

        if (null != auth) {
            SecurityContextHolder.getContext().setAuthentication(auth);
            return auth;
        } else {
            try {
                if (authentication instanceof UsernamePasswordAuthenticationToken)
                    userName = (String) authentication.getPrincipal();

                if (userName != null) {
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

                auth = super.authenticate(authentication);
                SecurityContextHolder.getContext().setAuthentication(auth);

                userCache.put(userKey, auth);

                return auth;
            } catch (BadCredentialsException e) {
                if (userName != null && managedUser != null) {
                    managedUser.increaseWrongTime();
                    userService.updateUser(managedUser);
                }
                throw e;
            }
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.equals(UsernamePasswordAuthenticationToken.class);
    }
}
