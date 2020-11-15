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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
public class MaintainModeTool extends ExecutableApplication {
    private boolean maintainModeOn;
    private String reason;
    private List<String> projects = Lists.newArrayList();
    private String owner;
    private KylinConfig config;
    private EpochManager epochManager;
    private boolean hiddenOutput;

    public MaintainModeTool() {
    }

    public MaintainModeTool(String reason) {
        this.reason = reason;
        this.hiddenOutput = true;
    }

    private static final Option OPTION_MaintainMode_ON = new Option("on", "on", false, "turn on maintain mode.");
    private static final Option OPTION_MaintainMode_ON_REASON = new Option("reason", "reason", true,
            "the reason to turn on maintain mode.");
    private static final Option OPTION_MaintainMode_OFF = new Option("off", "off", false, "turn off maintain mode.");
    private static final Option OPTION_PROJECTS = new Option("p", "projects", true,
            "specify projects to turn on or turn off maintain mode.");
    private static final Option OPTION_HELP = new Option("h", "help", false, "print help message.");
    private static final String LEADER_RACE_KEY = "kylin.server.leader-race.enabled";
    private static final Option OPTION_HIDDEN_OUTPUT = new Option("hidden", "hidden-output", true, "only show output in logs");

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_MaintainMode_ON);
        options.addOption(OPTION_MaintainMode_ON_REASON);
        options.addOption(OPTION_PROJECTS);
        options.addOption(OPTION_HELP);
        options.addOption(OPTION_MaintainMode_OFF);
        options.addOption(OPTION_HIDDEN_OUTPUT);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        initOptionValues(optionsHelper);
        init();
        if (maintainModeOn) {
            markEpochs();
        } else {
            releaseEpochs();
        }
    }

    public void init() {
        config = KylinConfig.getInstanceFromEnv();
        String ipAndPort = AddressUtil.getMockPortAddress();
        ResourceStore metaStore = ResourceStore.getKylinMetaStore(config);
        metaStore.getAuditLogStore().setInstance(ipAndPort);
        if (CollectionUtils.isEmpty(projects)) {
            projects = NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                    .collect(Collectors.toList());
        }
        owner = ipAndPort + "|" + Long.MAX_VALUE;
        projects.add(UnitOfWork.GLOBAL_UNIT);
        epochManager = EpochManager.getInstance(config);
        epochManager.setIdentity(owner);
    }

    public void markEpochs() {
        print("Start to mark epoch with reason: " + reason);
        Pair<Boolean, String> maintenanceModeDetail = checkMaintenanceMode(false);
        if (maintenanceModeDetail.getFirst()) {
            System.out.println(String.format("The system is under maintenance mode for [%s]. Please try again later.", maintenanceModeDetail.getSecond()));
            log.warn("The system is under maintenance mode for [{}]. Please try again later.", maintenanceModeDetail.getSecond());
            System.exit(1);
        }
        System.setProperty(LEADER_RACE_KEY, "false");
        ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        try {
            store.getAuditLogStore().catchupWithTimeout();
            for (String prj : projects) {
                markOwnerWithRetry(config.getTurnMaintainModeRetryTimes(), store, prj);
            }
        } catch (Exception e) {
            log.error("Mark epoch failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Turn on maintain mode failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
            System.clearProperty(LEADER_RACE_KEY);
            System.exit(1);
        }

        try {
            store.getAuditLogStore().catchupWithTimeout();
        } catch (Exception e) {
            log.error("Catchup audit log failed, try to release epochs", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Turn on maintain mode failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
            System.clearProperty(LEADER_RACE_KEY);
            releaseEpochs();
            System.exit(1);
        } finally {
            System.clearProperty(LEADER_RACE_KEY);
        }
        print("Mark epoch success with reason: " + reason);
    }

    private void markOwnerWithRetry(int retryTimes, ResourceStore store, String prj) throws Exception {
        boolean updateEpoch = epochManager.forceUpdateEpoch(prj, reason);
        if (!updateEpoch) {
            if (retryTimes > 0) {
                TimeUnit.SECONDS.sleep(2);
                store.getAuditLogStore().catchupWithTimeout();
                markOwnerWithRetry(retryTimes - 1, store, prj);
            } else {
                throw new IllegalStateException("Failed to turn on maintain mode due to project " + prj);
            }
        }
    }

    public void releaseEpochs() {
        try {
            print("Start to release epoch");
            Pair<Boolean, String> maintenanceModeDetail = checkMaintenanceMode(true);
            if (!maintenanceModeDetail.getFirst()) {
                System.out.println("System is not in maintenance mode.");
                log.warn("System is not in maintenance mode.");
                System.exit(1);
            }
            System.setProperty(LEADER_RACE_KEY, "true");
            epochManager.setIdentity("");
            for (val prj : projects) {
                epochManager.forceUpdateEpoch(prj, null);
            }
        } catch (Exception e) {
            log.error("Release epoch failed， try to turn off maintain mode manually.", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Turn off maintain mode failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
            System.clearProperty(LEADER_RACE_KEY);
            System.exit(1);
        } finally {
            System.clearProperty(LEADER_RACE_KEY);
        }
        print("Release epoch success");
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }

    private void initOptionValues(OptionsHelper optionsHelper) {
        if (optionsHelper.hasOption(OPTION_MaintainMode_ON) && optionsHelper.hasOption(OPTION_MaintainMode_OFF))
            throw new IllegalStateException("Can not turn on and off maintain mode at same time.");
        if (!optionsHelper.hasOption(OPTION_MaintainMode_ON) && !optionsHelper.hasOption(OPTION_MaintainMode_OFF))
            throw new IllegalStateException("You should specified turn on or off maintain mode.");
        this.maintainModeOn = optionsHelper.hasOption(OPTION_MaintainMode_ON);
        if (optionsHelper.hasOption(OPTION_PROJECTS)) {
            this.projects = Lists.newArrayList(optionsHelper.getOptionValue(OPTION_PROJECTS).split(","));
        }
        if (optionsHelper.hasOption(OPTION_HIDDEN_OUTPUT)) {
            this.hiddenOutput = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_HIDDEN_OUTPUT));
        }
        this.reason = optionsHelper.getOptionValue(OPTION_MaintainMode_ON_REASON);
        if (maintainModeOn && StringUtils.isEmpty(reason)) {
            log.warn("You need to use the argument -reason to explain why you turn on maintenance mode");
            System.out.println("You need to use the argument -reason to explain why you turn on maintenance mode");
            System.exit(1);
        }
        log.info("MaintainModeTool has option maintain mode on: {}{}{}", maintainModeOn,
                (StringUtils.isEmpty(reason) ? "" : " reason: " + reason),
                (projects.size() > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : ""));
        print(String.format("MaintainModeTool has option maintain mode on: %s%s%s", maintainModeOn,
                (StringUtils.isEmpty(reason) ? "" : " reason: " + reason),
                (projects.size() > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : "")));
    }

    private Pair<Boolean, String> checkMaintenanceMode(boolean needInMaintenanceMode) {
        for (String project : projects) {
            if (epochManager.getEpoch(project) != null) {
                Pair<Boolean, String> maintenanceModeDetail = epochManager.getMaintenanceModeDetail(project);
                if (maintenanceModeDetail.getFirst() != needInMaintenanceMode) {
                    return maintenanceModeDetail;
                }
            }
        }

        return Pair.newPair(needInMaintenanceMode, null);
    }

    private void print(String output) {
        if (!hiddenOutput) {
            System.out.println(output);
        }
        log.info(output);
    }

    public static void main(String[] args) {
        MaintainModeTool tool = new MaintainModeTool();
        tool.execute(args);
        System.exit(0);
    }
}