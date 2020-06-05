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
package io.kyligence.kap.tool.daemon.checker;

import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.CheckStateEnum;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KEProcessChecker extends AbstractHealthChecker {
    private static final Logger logger = LoggerFactory.getLogger(KEProcessChecker.class);

    public KEProcessChecker() {
        setPriority(0);
    }

    public String getProcessStatusCmd() {
        return "sh " + getKylinHome() + "/sbin/guardian-get-process-status.sh";
    }

    @Override
    CheckResult doCheck() {
        /*
        0: ke is running
        1: ke is stopped
        -1 ke is crashed
         */
        String cmd = getProcessStatusCmd();
        try {
            CliCommandExecutor.CliCmdExecResult result = getCommandExecutor().execute(cmd, null);
            int status = Integer.parseInt(result.getCmd().substring(0, result.getCmd().lastIndexOf('\n')));

            switch (status) {
            case 0:
                return new CheckResult(CheckStateEnum.NORMAL);
            case 1:
                return new CheckResult(CheckStateEnum.SUICIDE, "KE instance is normally stopped");
            case -1:
                return new CheckResult(CheckStateEnum.RESTART, "KE Instance is crashed");
            default:
                return new CheckResult(CheckStateEnum.WARN, "Unknown ke process status");
            }
        } catch (Exception e) {
            logger.error("Check KE process failed, cmd: {}", cmd, e);

            return new CheckResult(CheckStateEnum.WARN,
                    "Execute shell guardian-get-process-status.sh failed. " + e.getMessage());
        }
    }
}
