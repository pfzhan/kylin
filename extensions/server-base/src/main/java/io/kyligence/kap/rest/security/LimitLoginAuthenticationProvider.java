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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;


public class LimitLoginAuthenticationProvider extends DaoAuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(LimitLoginAuthenticationProvider.class);

    @Autowired
    private KapAuthenticationManager kapAuthenticationManager;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String userName = null;
        Authentication auth = null;

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

            auth = super.authenticate(authentication);
            return auth;
        } catch (BadCredentialsException e) {
            kapAuthenticationManager.increaseWrongTime(userName);
            throw e;
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.equals(UsernamePasswordAuthenticationToken.class);
    }
}
