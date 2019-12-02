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

package io.kyligence.kap.tool.metadata;

import static io.kyligence.kap.tool.util.ServiceDiscoveryUtil.runWithCurator;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.flywaydb.core.Flyway;

import io.kyligence.kap.tool.metadata.migration.V2__IndexesAndRecommendation;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.api.MigrationInfo;

@Slf4j
public class UpgradeMetadataCLI extends ExecutableApplication {

    private static final Option OPTION_INFO_ONLY = OptionBuilder.withDescription("Show migrate info do not migrate it")
            .hasArg(false).create("info");

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_INFO_ONLY);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val metadataUrl = kylinConfig.getMetadataUrl();
        val prefix = metadataUrl.getIdentifier();
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        val flyway = Flyway.configure() //
                .table(prefix + "_schema_history") //
                .dataSource(metadataUrl.getParameter("url"), metadataUrl.getParameter("username"),
                        metadataUrl.getParameter("password"))
                .baselineVersion("1") //
                .baselineOnMigrate(true) //
                .javaMigrations(new V2__IndexesAndRecommendation(resourceStore, prefix)) //
                .load();
        if (optionsHelper.hasOption(OPTION_INFO_ONLY)) {
            flyway.baseline();
            val info = flyway.info();
            log.info("current version is {}, last version is {}, pending size is {}",
                    info.current() == null ? null : info.current().getVersion(),
                    info.all()[info.all().length - 1].getVersion(), info.pending().length);
            for (MigrationInfo migrationInfo : info.all()) {
                log.info("Version: {}, Desc: {}, Stat: {}", migrationInfo.getVersion(), migrationInfo.getDescription(),
                        migrationInfo.getState());
            }
            System.exit(info.pending().length == 0 ? 0 : 2);
        } else {
            flyway.migrate();
        }
    }

    private boolean isInfoOnly() {
        return getOptions().hasOption(OPTION_INFO_ONLY.getOpt());
    }

    public static void main(String[] args) {
        val cli = new UpgradeMetadataCLI();
        int retCode = runWithCurator((isLocal, address) -> {
            if (isLocal || cli.isInfoOnly()) {
                cli.execute(args);
                return 0;
            }
            log.warn("Fail to migrate, please stop all job nodes first");
            return 1;

        });
        System.exit(retCode);
    }
}
