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

import io.kyligence.kap.common.logging.SetLogCategory;
import io.kyligence.kap.common.util.AddressUtil;
import org.apache.kylin.common.util.DateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class SecurityLoggerUtils {
    private static final Logger securityLogger = LoggerFactory.getLogger(SecurityLoggerUtils.SECURITY_LOG_APPENDER);

    private SecurityLoggerUtils() {
    }

    public static final String SECURITY_LOG_APPENDER = "security";
    private static final String LOGIN = "[Operation: login] user:%s, login time:%s, success:%s, ip and port:%s";
    private static final String LOGOUT = "[Operation: log out] user:%s, logout time:%s, ip and port:%s";

    public static void recordLoginSuccess(String username) {
        String loginSuccessMsg = String.format(Locale.ROOT, SecurityLoggerUtils.LOGIN, username,
                DateFormat.formatToTimeWithoutMilliStr(System.currentTimeMillis()), Boolean.TRUE,
                AddressUtil.getLocalInstance());
        try (SetLogCategory logCategory = new SetLogCategory(SecurityLoggerUtils.SECURITY_LOG_APPENDER)) {
            securityLogger.info(loginSuccessMsg);
        }
    }

    public static void recordLoginFailed(String username, Exception e) {
        String loginErrorMsg = String.format(Locale.ROOT, SecurityLoggerUtils.LOGIN, username,
                DateFormat.formatToTimeWithoutMilliStr(System.currentTimeMillis()), Boolean.FALSE,
                AddressUtil.getLocalInstance());
        try (SetLogCategory logCategory = new SetLogCategory(SecurityLoggerUtils.SECURITY_LOG_APPENDER)) {
            securityLogger.error(loginErrorMsg, e);
        }
    }

    public static void recordLogout(String username) {
        String logoutMessage = String.format(Locale.ROOT, SecurityLoggerUtils.LOGOUT, username,
                DateFormat.formatToTimeWithoutMilliStr(System.currentTimeMillis()), AddressUtil.getLocalInstance());
        try (SetLogCategory logCategory = new SetLogCategory(SecurityLoggerUtils.SECURITY_LOG_APPENDER)) {
            securityLogger.info(logoutMessage);
        }
    }
}
