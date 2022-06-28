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
import static io.kyligence.kap.tool.util.ScreenPrintUtil.printlnRed;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.OptionBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class CheckProjectModeCLI extends ExecutableApplication {

    private static final Option OPTION_METADATA_DIR = OptionBuilder.getInstance().hasArg().withArgName("metadata_dir")
            .withDescription("metadata dir.").isRequired(true).withLongOpt("metadata_dir").create("d");

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    public static void main(String[] args) {
        CheckProjectModeCLI updateProjectModelCLI = new CheckProjectModeCLI();
        try {
            updateProjectModelCLI.execute(args);
        } catch (RuntimeException e) {
            log.error("Failed to exec CheckProjectModeCLI", e);
            systemExitWhenMainThread(1);
        }

        log.info("Upgrade project mode finished!");
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

        List<ProjectInstance> projectInstanceList = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(projectInstanceList)) {
            printlnRed(String.format(Locale.ROOT, "found %d projects %s need to be changed to AI augmented mode.",
                    projectInstanceList.size(),
                    Arrays.toString(projectInstanceList.stream().map(ProjectInstance::getName).toArray())));

            if (optionsHelper.hasOption(OPTION_EXEC)) {
                throw new RuntimeException("Smart mode project exist, stop upgrade.");
            }
        } else {
            printlnGreen("found 0 projects need to be changed.");
            if (optionsHelper.hasOption(OPTION_EXEC)) {
                printlnGreen("projects mode upgrade succeeded.");
            }
        }
        log.info("Succeed to check project mode.");
    }
}
