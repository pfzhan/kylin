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

import static org.apache.kylin.rest.exception.ServerErrorCode.USER_LOCKED;
import static org.apache.kylin.rest.exception.ServerErrorCode.USER_UNAUTHORIZED;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;

import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.restclient.RestClient;
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
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;

public class LimitLoginAuthenticationProvider extends DaoAuthenticationProvider {

    private static final Logger limitLoginLogger = LoggerFactory.getLogger(LimitLoginAuthenticationProvider.class);

    @Autowired
    @Qualifier("userService")
    UserService userService;

    private ConcurrentHashMap<String, RestClient> clientMap = new ConcurrentHashMap<>();

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to init Message Digest ", e);
        }

        md.reset();

        ManagedUser managedUser = null;
        String userName = null;

        try {
            if (authentication instanceof UsernamePasswordAuthenticationToken)
                userName = (String) authentication.getPrincipal();

            if (userName != null) {
                NKylinUserManager userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
                managedUser = userManager.getIgnoreCase(userName);
                if (managedUser != null) {
                    userName = managedUser.getUsername();
                    authentication = new UsernamePasswordAuthenticationToken(userName, authentication.getCredentials());
                } else {
                    managedUser = (ManagedUser) userService.loadUserByUsername(userName);
                }
                Preconditions.checkNotNull(managedUser);
            }
            updateUserLockStatus(managedUser, userName);
            Authentication auth = super.authenticate(authentication);

            if (managedUser != null)
                managedUser.clearAuthenticateFailedRecord();

            SecurityContextHolder.getContext().setAuthentication(auth);

            return auth;
        } catch (BadCredentialsException e) {
            authenticateFail(managedUser, userName);
            if (managedUser != null && managedUser.isLocked()) {
                limitLoginLogger.error(MsgPicker.getMsg().getUSER_BE_LOCKED(), new KylinException(USER_LOCKED, e));
                throw new BadCredentialsException(MsgPicker.getMsg().getUSER_BE_LOCKED(),
                        new KylinException(USER_LOCKED, e));
            } else {
                limitLoginLogger.error(MsgPicker.getMsg().getUSER_AUTH_FAILED());
                throw new BadCredentialsException(MsgPicker.getMsg().getUSER_AUTH_FAILED());
            }
        } catch (UsernameNotFoundException e) {
            throw new BadCredentialsException(MsgPicker.getMsg().getUSER_AUTH_FAILED(),
                    new KylinException(USER_UNAUTHORIZED, MsgPicker.getMsg().getUSER_AUTH_FAILED()));
        }
    }

    private void authenticateFail(ManagedUser managedUser, String userName) {
        if (userName != null && managedUser != null) {
            managedUser.authenticateFail();
            updateUser(managedUser);
        }
    }

    private void updateUser(ManagedUser managedUser) {
            boolean isOwner = false;
            EpochManager manager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());
            try {
                isOwner = manager.checkEpochOwner(EpochManager.GLOBAL);
            } catch (Exception e) {
                logger.error("Get global epoch owner failed, update locally.", e);
                return;
            }
            if (isOwner) {
                userService.updateUser(managedUser);
            } else {
                try {
                    String owner = manager.getEpochOwner(EpochManager.GLOBAL).split("\\|")[0];
                    if (clientMap.get(owner) == null) {
                        clientMap.clear();
                        clientMap.put(owner, new RestClient(owner));
                    }
                    clientMap.get(owner).updateUser(managedUser);
                } catch (Exception e) {
                    logger.error("Failed to update user throw restclient", e);
                }
            }
    }

    private void updateUserLockStatus(ManagedUser managedUser, String userName) {
        if (managedUser != null && managedUser.isLocked()) {
            long lockedTime = managedUser.getLockedTime();
            long timeDiff = System.currentTimeMillis() - lockedTime;

            if (timeDiff > 30000) {
                managedUser.setLocked(false);
                updateUser(managedUser);
            } else {
                int leftSeconds = (30 - timeDiff / 1000) <= 0 ? 1 : (int) (30 - timeDiff / 1000);
                String msg = String.format(MsgPicker.getMsg().getUSER_IN_LOCKED_STATUS(), userName, leftSeconds);
                throw new LockedException(msg);
            }
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.equals(UsernamePasswordAuthenticationToken.class);
    }
}
