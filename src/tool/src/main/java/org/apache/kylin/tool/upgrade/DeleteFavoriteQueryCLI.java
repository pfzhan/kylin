/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.tool.util.MetadataUtil.getMetadataUrl;
import static org.apache.kylin.tool.util.ScreenPrintUtil.printlnGreen;
import static org.apache.kylin.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.project.NProjectManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class DeleteFavoriteQueryCLI extends ExecutableApplication {

    private static final Option OPTION_METADATA_DIR = OptionBuilder.getInstance().hasArg().withArgName("metadata_dir")
            .withDescription("metadata dir.").isRequired(true).withLongOpt("metadata_dir").create("d");

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    public static void main(String[] args) {
        DeleteFavoriteQueryCLI updateFQCLI = new DeleteFavoriteQueryCLI();
        try {
            updateFQCLI.execute(args);
        } catch (Exception e) {
            log.error("Failed to exec DeleteFavoriteQueryCLI", e);
            systemExitWhenMainThread(1);
        }

        log.info("Upgrade favorite query finished.");
        systemExitWhenMainThread(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_METADATA_DIR);
        options.addOption(OPTION_EXEC);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_METADATA_DIR));
        Preconditions.checkArgument(StringUtils.isNotBlank(metadataUrl));

        KylinConfig systemKylinConfig = KylinConfig.getInstanceFromEnv();
        systemKylinConfig.setMetadataUrl(metadataUrl);

        log.info("Start to truncate favorite query.");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        Map<String, List<FavoriteRule>> globalFavoriteRuleList = Maps.newHashMap();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.listAllProjects().forEach(projectInstance -> {
            FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(kylinConfig,
                    projectInstance.getName());
            List<FavoriteRule> favoriteRuleList = favoriteRuleManager.getAll();

            globalFavoriteRuleList.put(projectInstance.getName(), favoriteRuleList);
        });

        long frCount = globalFavoriteRuleList.values().stream().mapToLong(List::size).sum();
        printlnGreen(String.format(Locale.ROOT, "found %d recommendation metadata need to be updated.", frCount));

        if (optionsHelper.hasOption(OPTION_EXEC)) {
            globalFavoriteRuleList.forEach((project, frList) -> UnitOfWork.doInTransactionWithRetry(() -> {
                frList.forEach(
                        fr -> FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project).delete(fr));
                return null;
            }, project));
            printlnGreen("recommendation metadata upgrade succeeded.");

        }
        log.info("Succeed to truncate favorite query.");
    }
}
