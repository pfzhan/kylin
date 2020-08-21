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
package io.kyligence.kap.tool.upgrade;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.tool.OptionBuilder;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static io.kyligence.kap.tool.util.MetadataUtil.getMetadataUrl;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.println;

@Slf4j
public class UpdateProjectCLI extends ExecutableApplication implements IKeep {
    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("dir")
            .withDescription("Specify the directory to operator").isRequired(true).create("dir");

    private static final Option OPTION_HELP = OptionBuilder.getInstance().hasArg(false)
            .withDescription("print help message.").isRequired(false).withLongOpt("help").create("h");

    private static final Map<String, String> REPLACE_OVERRIDE_PROPERTIES_MAP = new HashMap<>();

    private KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

    static {
        REPLACE_OVERRIDE_PROPERTIES_MAP.put("kap.metadata.semi-automatic-mode", "kylin.metadata.semi-automatic-mode");
        REPLACE_OVERRIDE_PROPERTIES_MAP.put("kap.query.metadata.expose-computed-column",
                "kylin.query.metadata.expose-computed-column");
    }

    public static void main(String[] args) throws Exception {
        val tool = new UpdateProjectCLI();
        tool.execute(args);
        println("Update project finished.");
        System.exit(0);
    }

    void updateAllProjects(KylinConfig kylinConfig) {
        log.info("Start to update project...");
        val projectManager = NProjectManager.getInstance(kylinConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            updateProject(project);
        }
        log.info("Update project finished!");
    }

    private void updateProject(ProjectInstance project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NProjectManager npr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            // for upgrade, set project override properties
            LinkedHashMap<String, String> overrideKylinProps = project.getOverrideKylinProps();
            REPLACE_OVERRIDE_PROPERTIES_MAP.forEach((originKey, destKey) -> {
                String value = overrideKylinProps.get(originKey);
                if (StringUtils.isNotEmpty(value)) {
                    overrideKylinProps.remove(originKey);
                    overrideKylinProps.put(destKey, value);
                }
            });

            // for upgrade, set default database
            if (StringUtils.isEmpty(project.getDefaultDatabase())) {
                val schemaMap = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName())
                        .listTablesGroupBySchema();
                String defaultDatabase = DatabaseDesc.getDefaultDatabaseByMaxTables(schemaMap);
                project.setDefaultDatabase(defaultDatabase.toUpperCase());
            }
            npr.updateProject(project);
            return 0;
        }, project.getName(), 1);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_HELP);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        log.info("Start to update project...");

        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_DIR));
        kylinConfig.setMetadataUrl(metadataUrl);

        UpdateProjectCLI defaultDatabaseCLI = new UpdateProjectCLI();
        defaultDatabaseCLI.updateAllProjects(kylinConfig);
        log.info("Update project finished!");
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }
}
