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
package io.kyligence.kap.tool.daemon;

import lombok.Getter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.CliCommandExecutor;

import javax.crypto.SecretKey;

public class Worker {

    @Getter
    private static KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

    @Getter
    private static RestClient restClient;

    @Getter
    private static CliCommandExecutor commandExecutor;

    @Getter
    private static SecretKey kgSecretKey;

    @Getter
    private static String KE_PID;

    public synchronized void setKEPid(String pid) {
        KE_PID = pid;
    }

    static {
        int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
        restClient = new RestClient("127.0.0.1", serverPort, null, null);

        commandExecutor = new CliCommandExecutor();
    }

    public static String getKylinHome() {
        return KylinConfig.getKylinHome();
    }

    public static String getServerPort() {
        return kylinConfig.getServerPort();
    }

    public synchronized void setKgSecretKey(SecretKey secretKey) {
        kgSecretKey = secretKey;
    }

}
