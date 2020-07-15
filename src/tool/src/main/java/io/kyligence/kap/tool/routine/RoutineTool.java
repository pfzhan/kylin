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
package io.kyligence.kap.tool.routine;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.CuratorOperator;
import io.kyligence.kap.tool.garbage.GarbageCleaner;
import io.kyligence.kap.tool.garbage.SourceUsageCleaner;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

@Getter
@Slf4j
public class RoutineTool extends ExecutableApplication implements IKeep {
    private boolean storageCleanup;
    private boolean metadataCleanup;
    private String[] projects = new String[0];

    private static final Option OPTION_CLEANUP_METADATA = new Option("m", "metadata", false, "cleanup metadata garbage after check.");
    private static final Option OPTION_CLEANUP = new Option("c", "cleanup", false, "cleanup hdfs garbage after check.");
    private static final Option OPTION_PROJECTS = new Option("p", "projects", true, "specify projects to cleanup.");
    private static final Option OPTION_HELP = new Option("h", "help", false, "print help message.");

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_CLEANUP);
        options.addOption(OPTION_PROJECTS);
        options.addOption(OPTION_HELP);
        options.addOption(OPTION_CLEANUP_METADATA);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            if (new CuratorOperator().isJobNodeExist()) {
                System.out.println("Please confirm that no job/all node is running!");
                return;
            }
            Scanner scanner = new Scanner(System.in);
            System.out.println("No job/all node is running?:(Y/N)");
            String s = scanner.next();
            if (!s.equalsIgnoreCase("Y")) {
                System.out.println("Job/all node should not be running when execute routintool!");
                return;
            }
        }
        if (printUsage(optionsHelper)) {
            return;
        }
        initOptionValues(optionsHelper);

        if (metadataCleanup) {
            try {
                System.out.println("Start to cleanup metadata");
                List<String> projectsToCleanup = Arrays.asList(projects);
                if (projectsToCleanup.isEmpty()) {
                    projectsToCleanup = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects()
                            .stream().map(ProjectInstance::getName).collect(Collectors.toList());
                }
                new SourceUsageCleaner().cleanup();
                for (String projName : projectsToCleanup) {
                    try {
                        GarbageCleaner.unsafeCleanupMetadataManually(projName);
                    } catch (Exception e) {
                        log.error("Project[{}] cleanup Metadata failed", projName, e);
                    }
                }
                System.out.println("Metadata cleanup finished");
            } catch (Exception e) {
                log.error("Metadata cleanup failed", e);
                System.out.println(StorageCleaner.ANSI_RED
                        + "Metadata cleanup failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                        + StorageCleaner.ANSI_RESET);
            }
        }

        try {
            StorageCleaner storageCleaner = new StorageCleaner(storageCleanup, Arrays.asList(projects));
            System.out.println("Start to cleanup HDFS");
            storageCleaner.execute();
            System.out.println("cleanup HDFS finished");
        } catch (Exception e) {
            log.error("cleanup HDFS failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "cleanup HDFS failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        }
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }

    private void initOptionValues(OptionsHelper optionsHelper) {
        this.storageCleanup = optionsHelper.hasOption(OPTION_CLEANUP);
        this.metadataCleanup = optionsHelper.hasOption(OPTION_CLEANUP_METADATA);

        if (optionsHelper.hasOption(OPTION_PROJECTS)) {
            this.projects = optionsHelper.getOptionValue(OPTION_PROJECTS).split(",");
        }
        log.info("RoutineTool has option metadata cleanup: " + metadataCleanup + " storage cleanup: " + storageCleanup + (projects.length > 0 ? " projects: "+ optionsHelper.getOptionValue(OPTION_PROJECTS) : ""));
        System.out.println("RoutineTool has option metadata cleanup: " + metadataCleanup + " storage cleanup: " + storageCleanup + (projects.length > 0 ? " projects: "+ optionsHelper.getOptionValue(OPTION_PROJECTS) : ""));
    }

    public static void main(String[] args) {
        RoutineTool tool = new RoutineTool();
        tool.execute(args);
        System.exit(0);
    }

}
