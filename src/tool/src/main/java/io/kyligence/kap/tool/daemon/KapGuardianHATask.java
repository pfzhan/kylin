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

import io.kyligence.kap.common.util.SecretKeyUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapGuardianHATask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KapGuardianHATask.class);
    private final CliCommandExecutor cli;

    private final String statusCmd;
    private final String startCmd;

    public KapGuardianHATask() {
        this.cli = new CliCommandExecutor();

        String kylinHome = KylinConfig.getKylinHome();
        String pidFile = kylinHome + "/kgid";

        String errLog = kylinHome + "/logs/shell.stderr";
        String outLog = kylinHome + "/logs/shell.stdout";

        this.statusCmd = "sh " + kylinHome + "/sbin/guardian-get-process-status.sh " + pidFile;
        this.startCmd = "nohup sh " + kylinHome + "/bin/guardian.sh start 2>>" + errLog + " | tee -a " + outLog + " &";

        initKGSecretKey();
    }

    private void initKGSecretKey() {
        if (KylinConfig.getInstanceFromEnv().isGuardianEnabled()) {
            try {
                SecretKeyUtil.initKGSecretKey();
            } catch (Exception e) {
                logger.error("init kg secret key failed!", e);
            }
        }
    }

    @Override
    public void run() {
        try {
            CliCommandExecutor.CliCmdExecResult result = cli.execute(statusCmd, null);
            // 0 running, 1 stopped, -1 crashed
            int status = Integer.parseInt(result.getCmd().substring(0, result.getCmd().lastIndexOf('\n')));

            // start kg
            if (0 != status) {
                if (1 == status) {
                    logger.info("Guardian Process is not running, try to start it");
                } else if (-1 == status) {
                    logger.info("Guardian Process is crashed, try to start it");
                }

                logger.info("Starting Guardian Process");

                cli.execute(startCmd, null);

                logger.info("Guardian Process started");
            } else {
                logger.info("Guardian Process is running");
            }
        } catch (Exception e) {
            logger.error("Failed to monitor Guardian Process!", e);
        }
    }
}
