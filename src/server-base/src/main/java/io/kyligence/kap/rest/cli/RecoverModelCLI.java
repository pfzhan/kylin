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

package io.kyligence.kap.rest.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

/**
 * recover a model broken due to event reasons
 */
public class RecoverModelCLI {

    @SuppressWarnings("static-access")
    private static final Option OPTION_METADATA = OptionBuilder.withArgName("metadata").hasArg().isRequired()
            .withDescription("Metadata").create("metadata");

    @SuppressWarnings("static-access")
    private static final Option OPTION_MODEL = OptionBuilder.withArgName("model").hasArg().isRequired()
            .withDescription("MODEL ALIAS").create("model_alias");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired()
            .withDescription("PROJECT").create("project");

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption(OPTION_METADATA);
        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_PROJECT);

        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);

        String modelAlias = commandLine.getOptionValue(OPTION_MODEL.getOpt());
        String project = commandLine.getOptionValue(OPTION_PROJECT.getOpt());

        KylinConfig config = KylinConfig.createInstanceFromUri(commandLine.getOptionValue(OPTION_METADATA.getOpt()));

        val dfManager = NDataflowManager.getInstance(config, project);

        val modelManager = NDataModelManager.getInstance(config, project);

        val df = dfManager.getDataflowByModelAlias(modelAlias);
        val model = modelManager.getDataModelDescByAlias(modelAlias);

        Preconditions.checkNotNull(model, "cannot find model with alias " + modelAlias);
        Preconditions.checkNotNull(df, "cannot find dataflow with alias " + modelAlias);

        if (df.getStatus().equals(RealizationStatusEnum.BROKEN) && df.isEventError()) {
            UnitOfWork.doInTransactionWithRetry(() -> {
                if (model.getManagementType().equals(ManagementType.MODEL_BASED)) {
                    RecoverModelUtil.recoverBySelf(df, config, project);
                } else {
                    RecoverModelUtil.recoverByDataloadingRange(df, config, project);
                }
                System.out.println("Recover has finished!");
                return null;
            }, project);

        } else {
            System.out.println("This model is healthy, no need to recover it!");
        }

    }
}
