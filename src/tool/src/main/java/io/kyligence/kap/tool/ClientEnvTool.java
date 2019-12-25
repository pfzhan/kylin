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

import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ClientEnvTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final String CLI_VERSION = "version";

    private static final String LINUX_DEFRAG = "/sys/kernel/mm/transparent_hugepage/defrag";
    private static final String LINUX_SWAP = "/proc/sys/vm/swappiness";
    private static final String LINUX_CPU = "/proc/cpuinfo";

    public ClientEnvTool() {
        super();
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) {
        // dump os info
        addFile(new File(LINUX_DEFRAG), new File(exportDir, "linux/transparent_hugepage"));
        addFile(new File(LINUX_SWAP), new File(exportDir, "linux/swappiness"));
        addFile(new File(LINUX_CPU), new File(exportDir, "linux"));

        File linuxDir = new File(exportDir, "linux");
        addShellOutput("lsb_release -a", linuxDir, "lsb_release");
        addShellOutput("df -h", linuxDir, "disk_usage");
        addShellOutput("free -m", linuxDir, "mem_usage_mb");
        addShellOutput("top -b -n 1 | head -n 30", linuxDir, "top");
        addShellOutput("ps aux|grep kylin", linuxDir, "kylin_processes");

        // dump hadoop env
        addShellOutput("hadoop version", new File(exportDir, "hadoop"), CLI_VERSION);
        addShellOutput("hive --version", new File(exportDir, "hive"), CLI_VERSION, false, true);
        addShellOutput("beeline --version", new File(exportDir, "hive"), "beeline_version", false, true);

        // include klist command output
        addShellOutput("klist", new File(exportDir, "kerberos"), "klist", false, true);
    }

    protected void extractInfoByCmd(String cmd, File destFile) {
        try {
            if (!destFile.exists() && !destFile.createNewFile()) {
                logger.error("Failed to createNewFile destFile.");
            }

            logger.info("The command is: {}", cmd);
            getCmdExecutor().execute(cmd, null);
        } catch (Exception e) {
            logger.error("Failed to execute copyCmd", e);
        }
    }

}
