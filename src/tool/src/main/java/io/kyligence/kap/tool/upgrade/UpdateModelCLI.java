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

import static io.kyligence.kap.metadata.cube.model.IndexEntity.TABLE_INDEX_START_ID;
import static io.kyligence.kap.tool.util.MetadataUtil.getMetadataUrl;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.printlnGreen;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.project.UnitOfAllWorks;
import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class UpdateModelCLI extends ExecutableApplication implements IKeep {

    private static final Option OPTION_METADATA_DIR = OptionBuilder.getInstance().hasArg().withArgName("metadata_dir")
            .withDescription("metadata dir.").isRequired(true).withLongOpt("metadata_dir").create("d");

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    public static void main(String[] args) {
        UpdateModelCLI updateModelCLI = new UpdateModelCLI();
        try {
            updateModelCLI.execute(args);
        } catch (Exception e) {
            log.error("Failed to exec UpdateModelCLI", e);
            systemExitWhenMainThread(1);
        }

        log.info("Upgrade model finished.");
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

        log.info("Start to upgrade all model.");
        UnitOfAllWorks.doInTransaction(() -> {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

            List<NDataModel> globalUpdateModelList = Lists.newArrayList();

            NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
            projectManager.listAllProjects().forEach(projectInstance -> {
                NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig,
                        projectInstance.getName());

                NDataModelManager dataModelManager = NDataModelManager.getInstance(kylinConfig,
                        projectInstance.getName());
                List<NDataModel> updateModelList = dataModelManager.listAllModels().stream()
                        .filter(nDataModel -> !nDataModel.isBroken())
                        .filter(nDataModel -> !indexPlanManager.getIndexPlan(nDataModel.getUuid()).isBroken())
                        .map(dataModelManager::copyForWrite).peek(model -> {
                            IndexPlan indexPlan = indexPlanManager.getIndexPlan(model.getUuid());

                            Set<Integer> tableIndexDimensions = indexPlan.getIndexes().stream()
                                    .filter(indexEntity -> indexEntity.getId() >= TABLE_INDEX_START_ID)
                                    .flatMap(indexEntity -> indexEntity.getDimensions().stream())
                                    .collect(Collectors.toSet());
                            long count = model.getAllNamedColumns().stream().filter(namedColumn -> {
                                if (!namedColumn.isDimension() && tableIndexDimensions.contains(namedColumn.getId())) {
                                    namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
                                    return true;
                                }
                                return false;
                            }).count();

                            log.info("Project: {}, model: {}, add model dimension size: {}", projectInstance.getName(),
                                    model.getAlias(), count);
                        }).collect(Collectors.toList());

                globalUpdateModelList.addAll(updateModelList);
            });

            printlnGreen(String.format("found %d models need to be modified.", globalUpdateModelList.size()));
            if (optionsHelper.hasOption(OPTION_EXEC)) {
                globalUpdateModelList.forEach(model -> NDataModelManager.getInstance(kylinConfig, model.getProject())
                        .updateDataModelDesc(model));
            }

            return null;
        }, false);

        if (optionsHelper.hasOption(OPTION_EXEC)) {
            printlnGreen("model dimensions upgrade succeeded.");
        }

        log.info("Succeed to upgrade all model.");
    }
}
