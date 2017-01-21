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

package io.kyligence.kap.rest.service;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.io.Files;

@Component("kyBotService")
public class KyBotService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(KyBotService.class);

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpLocalKyBotPackage(boolean needUpload) throws IOException {
        File exportPath = Files.createTempDir();
        String[] args = { "-all", exportPath.getAbsolutePath(), Boolean.toString(needUpload) };
        runKyBotCLI(args);
        return getKyBotPackagePath(exportPath);
    }

    private void runKyBotCLI(String[] args) throws IOException {
        File cwd = new File("");
        logger.info("Current path: " + cwd.getAbsolutePath());

        logger.info("KybotClientCLI args: " + Arrays.toString(args));
        File script = new File(KylinConfig.getKylinHome() + File.separator + "bin", "diag.sh");
        if (!script.exists()) {
            throw new RuntimeException("diag.sh not found at " + script.getAbsolutePath());
        }

        String diagCmd = script.getAbsolutePath() + " " + StringUtils.join(args, " ");
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        Pair<Integer, String> cmdOutput = executor.execute(diagCmd, new org.apache.kylin.common.util.Logger() {
            @Override
            public void log(String message) {
                logger.info(message);
            }
        });

        if (cmdOutput.getKey() != 0) {
            throw new RuntimeException("Failed to generate KyBot package.");
        }
    }

    private String getKyBotPackagePath(File destDir) {
        File[] files = destDir.listFiles();
        if (files == null) {
            throw new RuntimeException("KyBot package is not available in directory: " + destDir.getAbsolutePath());
        }
        for (File subDir : files) {
            if (subDir.isDirectory()) {
                for (File file : subDir.listFiles()) {
                    if (file.getName().endsWith(".zip")) {
                        return file.getAbsolutePath();
                    }
                }
            }
        }
        throw new RuntimeException("KyBot package not found in directory: " + destDir.getAbsolutePath());
    }
}
