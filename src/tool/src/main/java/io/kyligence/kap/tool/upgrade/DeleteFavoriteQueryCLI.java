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

import static io.kyligence.kap.tool.util.MetadataUtil.getMetadataUrl;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.printlnGreen;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class DeleteFavoriteQueryCLI extends ExecutableApplication implements IKeep {

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

        long fr = globalFavoriteRuleList.values().stream().mapToLong(List::size).sum();
        printlnGreen(String.format(Locale.ROOT, "found %d recommendation metadata need to be updated.", fr));

        if (optionsHelper.hasOption(OPTION_EXEC)) {
            UnitOfWork.doInTransactionWithRetry(()->{
                globalFavoriteRuleList.forEach((project, frList) -> {
                    frList.forEach(tfr -> FavoriteRuleManager.getInstance(kylinConfig, project).delete(tfr));
                    printlnGreen("recommendation metadata upgrade succeeded.");
                });
                return null;
            }, UnitOfWork.GLOBAL_UNIT);

        }
        log.info("Succeed to truncate favorite query.");
    }
}
