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

import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import org.springframework.util.CollectionUtils;

@Slf4j
@Setter
public class MaintainModeTool extends ExecutableApplication {
    private boolean maintainModeOn;
    private List<String> projects = Lists.newArrayList();
    private String owner;
    private KylinConfig config;
    private EpochManager epochManager;

    private static final Option OPTION_MaintainMode_ON = new Option("on", "on", false, "turn on maintain mode.");
    private static final Option OPTION_MaintainMode_OFF = new Option("off", "off", false, "turn off maintain mode.");
    private static final Option OPTION_PROJECTS = new Option("p", "projects", true,
            "specify projects to turn on or turn off maintain mode.");
    private static final Option OPTION_HELP = new Option("h", "help", false, "print help message.");

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_MaintainMode_ON);
        options.addOption(OPTION_PROJECTS);
        options.addOption(OPTION_HELP);
        options.addOption(OPTION_MaintainMode_OFF);
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

    public void init() throws UnknownHostException {
        config = KylinConfig.getInstanceFromEnv();
        String ipAndPort = AddressUtil.getMockPortAddress();
        ResourceStore.getKylinMetaStore(config).getAuditLogStore().setInstance(ipAndPort);
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
        try {
            System.out.println("Start to mark epoch");
            System.setProperty("kylin.server.leader-race.enabled", "false");
            for (val prj : projects) {
                epochManager.forceUpdateEpoch(prj);
            }
        } catch (Exception e) {
            log.error("Mark epoch failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Turn on maintain mode failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        } finally {
            System.clearProperty("kylin.server.leader-race.enabled");
        }
    }

    public void releaseEpochs() {
        try {
            System.setProperty("kylin.server.leader-race.enabled", "true");
            epochManager.setIdentity("");
            for (val prj : projects) {
                epochManager.forceUpdateEpoch(prj);
            }
        } catch (Exception e) {
            log.error("Release epoch failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Turn off maintain mode failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        } finally {
            System.clearProperty("kylin.server.leader-race.enabled");
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
        if (optionsHelper.hasOption(OPTION_MaintainMode_ON) && optionsHelper.hasOption(OPTION_MaintainMode_OFF))
            throw new IllegalStateException("Can not turn on and off maintain mode at same time.");
        if (!optionsHelper.hasOption(OPTION_MaintainMode_ON) && !optionsHelper.hasOption(OPTION_MaintainMode_OFF))
            throw new IllegalStateException("You should specified turn on or off maintain mode.");
        this.maintainModeOn = optionsHelper.hasOption(OPTION_MaintainMode_ON);
        if (optionsHelper.hasOption(OPTION_PROJECTS)) {
            this.projects = Lists.newArrayList(optionsHelper.getOptionValue(OPTION_PROJECTS).split(","));
        }
        log.info("MaintainModeTool has option maintain mode on: " + maintainModeOn
                + (projects.size() > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : ""));
        System.out.println("MaintainModeTool has option maintain mode on: " + maintainModeOn
                + (projects.size() > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : ""));
    }

    public static void main(String[] args) {
        MaintainModeTool tool = new MaintainModeTool();
        tool.execute(args);
        System.exit(0);
    }
}