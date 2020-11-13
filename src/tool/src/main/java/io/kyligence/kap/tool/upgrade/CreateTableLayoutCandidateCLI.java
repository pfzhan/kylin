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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.util.RawRecStoreUtil;
import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.tool.util.MetadataUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;

import javax.sql.DataSource;

import static io.kyligence.kap.tool.util.ScreenPrintUtil.printlnGreen;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.printlnRed;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class CreateTableLayoutCandidateCLI extends ExecutableApplication implements IKeep {

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    public static void main(String[] args) {
        CreateTableLayoutCandidateCLI createTableLayoutCandicateCLI = new CreateTableLayoutCandidateCLI();
        try {
            createTableLayoutCandicateCLI.execute(args);
        } catch (Exception e) {
            log.error("Failed to exec CreateTableLayoutCandidateCLI", e);
            systemExitWhenMainThread(1);
        }

        log.info("Upgrade table rec_candidate finished!");
        systemExitWhenMainThread(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_EXEC);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        KylinConfig systemKylinConfig = KylinConfig.getInstanceFromEnv();
        StorageURL url = systemKylinConfig.getMetadataUrl();
        String tableName = url.getIdentifier() + JdbcRawRecStore.RECOMMENDATION_CANDIDATE;

        DataSource dataSource = MetadataUtil.getDataSource(systemKylinConfig);

        boolean tableExist = false;
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            printlnGreen("found layout candidate table already exists.");
            tableExist = true;
        } else {
            printlnGreen("found layout candidate table doesn't exists.");
        }

        if (optionsHelper.hasOption(OPTION_EXEC)) {
            if (!tableExist) {
                printlnGreen("start to create layout candidate table.");

                try {
                    MetadataUtil.createTableIfNotExist((BasicDataSource) dataSource, tableName,
                            RawRecStoreUtil.CREATE_REC_TABLE, Lists.newArrayList(RawRecStoreUtil.CREATE_INDEX));
                } catch (Exception e) {
                    printlnRed("Failed to create layout candidate table.");
                    throw e;
                }
            }
            printlnGreen("layout candidate table upgrade succeeded.");
        }
    }
}
