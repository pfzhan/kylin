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
package io.kyligence.kap.tool.daemon.handler;

import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.HandleResult;
import io.kyligence.kap.tool.daemon.HandleStateEnum;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartStateHandler extends AbstractCheckStateHandler {
    private static final Logger logger = LoggerFactory.getLogger(RestartStateHandler.class);

    @Override
    public HandleResult doHandle(CheckResult checkResult) {
        logger.info("Start to restart instance port[{}] ...", getServerPort());
        String cmd = "nohup sh " + KylinConfig.getKylinHome() + "/bin/kylin.sh restart > /dev/null 2>&1 &";
        try {
            getCommandExecutor().execute(cmd, null);
            logger.info("Success to restart instance port[{}] ...", getServerPort());
        } catch (Exception e) {
            logger.error("Failed to restart the instance port [{}], cmd: {}", getServerPort(), cmd);
        }

        return new HandleResult(HandleStateEnum.STOP_CHECK);
    }
}
