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

package io.kyligence.kap.tool;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.util.Unsafe;

/**
 * When performing check-env, this class is used to check kylin config
 */
public class KylinConfigCheckCLI {

    private static final String SERVER_CONFIG_PREFIX = "server.";
    private static final String SPRING_CONFIG_PREFIX = "spring.";
    private static final String KYLIN_CONFIG_PREFIX = "kylin.";
    /**
     * Not recommended set the configuration items at the beginning with kap
     */
    private static final String KAP_CONFIG_PREFIX = "kap.";

    public static void main(String[] args) {
        execute();
        Unsafe.systemExit(0);
    }

    public static void execute() {
        Properties config = KylinConfig.newKylinConfig().exportToProperties();
        for (String key : config.stringPropertyNames()) {
            if (!StringUtils.startsWith(key, KYLIN_CONFIG_PREFIX) && !StringUtils.startsWith(key, SERVER_CONFIG_PREFIX)
                    && !StringUtils.startsWith(key, SPRING_CONFIG_PREFIX)
                    && !StringUtils.startsWith(key, KAP_CONFIG_PREFIX)) {
                System.out.println(key);
                break;
            }
        }
    }
}
