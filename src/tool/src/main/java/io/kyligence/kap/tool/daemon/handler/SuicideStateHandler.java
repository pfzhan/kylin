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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class SuicideStateHandler extends AbstractCheckStateHandler {
    private static final Logger logger = LoggerFactory.getLogger(SuicideStateHandler.class);

    @Override
    public HandleResult doHandle(CheckResult checkResult) {
        logger.info("Start to suicide ...");

        String pidFile = getKylinHome() + "/kgid";

        File file = new File(pidFile);
        try {
            if (Files.deleteIfExists(file.toPath())) {
                logger.info("Deleted file: {}", pidFile);
            } else {
                logger.warn("Can not delete the file: {}", pidFile);
            }
        } catch (IOException e) {
            logger.error("Failed to delete the file: {}", pidFile);
        }

        System.exit(0);
        return new HandleResult(HandleStateEnum.STOP_CHECK);
    }
}
