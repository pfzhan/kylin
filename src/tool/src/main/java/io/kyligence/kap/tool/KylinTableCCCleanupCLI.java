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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;

public class KylinTableCCCleanupCLI extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger(KylinTableCCCleanupCLI.class);

    private static final Option OPTION_CLEANUP;
    private static final Option OPTION_PROJECTS;
    private static final Option OPTION_HELP;

    private boolean cleanup = false;

    static {
        OPTION_CLEANUP = new Option("c", "cleanup", false, "Cleanup the computed columns in table metadata.");
        OPTION_PROJECTS = new Option("p", "projects", true, "Specify projects to cleanup.");
        OPTION_HELP = new Option("h", "help", false, "Print usage of KapTableCCCleanupCLI");
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_CLEANUP);
        options.addOption(OPTION_PROJECTS);
        options.addOption(OPTION_HELP);
        return options;
    }

    public static void main(String[] args) {
        int exit = 0;
        MaintainModeTool maintainModeTool = new MaintainModeTool("cleanup table cc");
        maintainModeTool.init();
        try {
            maintainModeTool.markEpochs();
            if (EpochManager.getInstance().isMaintenanceMode()) {
                Runtime.getRuntime().addShutdownHook(new Thread(maintainModeTool::releaseEpochs));
            }
            new KylinTableCCCleanupCLI().execute(args);
        } catch (Exception e) {
            exit = 1;
            logger.warn("Fail to cleanup table cc.", e);
        }
        Unsafe.systemExit(exit);
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        boolean hasOptionCleanup = optionsHelper.hasOption(OPTION_CLEANUP);
        if (hasOptionCleanup) {
            cleanup = true;
        }

        boolean hasOptionProjects = optionsHelper.hasOption(OPTION_PROJECTS);
        List<String> projects;
        if (hasOptionProjects) {
            projects = Arrays.asList(optionsHelper.getOptionValue(OPTION_PROJECTS).split(","));
        } else {
            projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects().stream()
                    .map(ProjectInstance::getName).collect(Collectors.toList());
        }
        logger.info("Cleanup option value: '{}', projects {}", cleanup, projects);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        KylinTableCCCleanup kapTableCCCleanup = new KylinTableCCCleanup(config, cleanup, projects);
        kapTableCCCleanup.scanAllTableCC();
        System.out.println("Done. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr");
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }
}
