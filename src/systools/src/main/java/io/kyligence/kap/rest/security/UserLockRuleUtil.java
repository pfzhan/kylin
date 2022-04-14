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

import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.user.ManagedUser;

import java.util.Map;

public class UserLockRuleUtil {

    private static Map<Integer, Long> lockDurationRules = Maps.newHashMap();

    static {
        // wrong time => lock duration (ms)
        lockDurationRules.put(0, 0L);
        lockDurationRules.put(1, 0L);
        lockDurationRules.put(2, 0L);
        lockDurationRules.put(3, 30 * 1000L); // 30s
        lockDurationRules.put(4, 60 * 1000L); // 1min
        lockDurationRules.put(5, 5 * 60 * 1000L); // 5min
        lockDurationRules.put(6, 10 * 60 * 1000L); // 10 min
        lockDurationRules.put(7, 30 * 60 * 1000L); // 30 min
        lockDurationRules.put(8, 24 * 3600 * 1000L); // 1d
        lockDurationRules.put(9, 72 * 3600 * 1000L); // 3d
        lockDurationRules.put(10, Long.MAX_VALUE); // lock permanently
    }

    public static long getLockDuration(int wrongTime) {
        if (wrongTime >= 0 && wrongTime <= 10) {
            return lockDurationRules.get(wrongTime);
        }
        return lockDurationRules.get(10);
    }

    public static long getLockDurationSeconds(int wrongTime) {
        long lockDurationMs = getLockDuration(wrongTime);
        return lockDurationMs / 1000;
    }

    public static long getLockDurationSeconds(ManagedUser managedUser) {
        return getLockDurationSeconds(managedUser.getWrongTime());
    }

    public static boolean isLockedPermanently(ManagedUser managedUser) {
        return Long.MAX_VALUE == lockDurationRules.get(managedUser.getWrongTime());
    }

    public static boolean isLockDurationEnded(ManagedUser managedUser, long duration) {
        return duration >= getLockDuration(managedUser.getWrongTime());
    }

    public static long getLockLeftSeconds(ManagedUser managedUser, long duration) {
        long lockDuration = getLockDuration(managedUser.getWrongTime());
        if (Long.MAX_VALUE == lockDuration) {
            return Long.MAX_VALUE;
        }
        long leftSeconds = (lockDuration - duration) / 1000;
        return leftSeconds <= 0 ? 1 : leftSeconds;
    }
}
