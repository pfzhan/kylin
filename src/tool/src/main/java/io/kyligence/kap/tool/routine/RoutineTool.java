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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.util.QueryHisStoreUtil;
import io.kyligence.kap.tool.MaintainModeTool;
import io.kyligence.kap.tool.garbage.GarbageCleaner;
import io.kyligence.kap.tool.garbage.SourceUsageCleaner;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class RoutineTool extends ExecutableApplication implements IKeep {
    private boolean storageCleanup;
    private boolean metadataCleanup;
    private String[] projects = new String[0];

    private static final Option OPTION_CLEANUP_METADATA = new Option("m", "metadata", false,
            "cleanup metadata garbage after check.");
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
        if (printUsage(optionsHelper)) {
            return;
        }
        initOptionValues(optionsHelper);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> instances = NProjectManager.getInstance(kylinConfig).listAllProjects();
        System.out.println("Start to cleanup metadata");
        List<String> projectsToCleanup = Arrays.asList(projects);
        if (projectsToCleanup.isEmpty()) {
            projectsToCleanup = instances.stream().map(ProjectInstance::getName).collect(Collectors.toList());
        }
        MaintainModeTool maintainModeTool = new MaintainModeTool("routine tool");
        maintainModeTool.init();
        maintainModeTool.markEpochs();
        if (EpochManager.getInstance(kylinConfig).isMaintenanceMode()) {
            Runtime.getRuntime().addShutdownHook(new Thread(maintainModeTool::releaseEpochs));
        }
        doCleanup(projectsToCleanup);
    }

    private void doCleanup(List<String> projectsToCleanup) {
        try {
            if (metadataCleanup) {
                cleanMeta(projectsToCleanup);
            }
            cleanStorage();
        } catch (Exception e) {
            log.error("Failed to execute routintool", e);
        }
    }

    private void cleanMeta(List<String> projectsToCleanup) throws IOException {
        try {
            cleanGlobalMeta();
            for (String projName : projectsToCleanup) {
                cleanMetaByProject(projName);
            }
            cleanQueryHistories();
            deleteRawRecItems();
            System.out.println("Metadata cleanup finished");
        } catch (Exception e) {
            log.error("Metadata cleanup failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Metadata cleanup failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        }

    }

    public void cleanGlobalMeta() {
        log.info("Start to clean up global meta");
        try {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                new SourceUsageCleaner().cleanup();
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (Exception e) {
            log.error("Failed to clean global meta", e);
        }
        log.info("Clean up global meta finished");

    }

    public void cleanMetaByProject(String projectName) {
        log.info("Start to clean up {} meta", projectName);
        try {
            GarbageCleaner.unsafeCleanupMetadataManually(projectName);
        } catch (Exception e) {
            log.error("Project[{}] cleanup Metadata failed", projectName, e);
        }
        log.info("Clean up {} meta finished", projectName);
    }

    public void cleanStorage() {
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
        log.info("RoutineTool has option metadata cleanup: " + metadataCleanup + " storage cleanup: " + storageCleanup
                + (projects.length > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : ""));
        System.out.println(
                "RoutineTool has option metadata cleanup: " + metadataCleanup + " storage cleanup: " + storageCleanup
                        + (projects.length > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : ""));
    }

    public static void main(String[] args) {
        RoutineTool tool = new RoutineTool();
        tool.execute(args);
        Unsafe.systemExit(0);
    }

    public void setProjects(String[] projects) {
        this.projects = projects;
    }

    public void setStorageCleanup(boolean storageCleanup) {
        this.storageCleanup = storageCleanup;
    }

    public static void deleteRawRecItems() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(projectInstance -> !projectInstance.isExpertMode()).collect(Collectors.toList());
        Thread.currentThread().setName("DeleteRawRecItemsInDB");
        for (ProjectInstance instance : projectInstances) {
            try {
                RawRecManager rawRecManager = RawRecManager.getInstance(instance.getName());
                rawRecManager.deleteAllOutDated(instance.getName());
                Set<String> modelIds = NDataModelManager.getInstance(config, instance.getName()).listAllModelIds();
                rawRecManager.deleteRecItemsOfNonExistModels(instance.getName(), modelIds);
            } catch (Exception e) {
                log.error("project<" + instance.getName() + "> delete raw recommendations in DB failed", e);
            }
        }
    }

    public static void cleanQueryHistories() {
        QueryHisStoreUtil.cleanQueryHistory();
    }
}
