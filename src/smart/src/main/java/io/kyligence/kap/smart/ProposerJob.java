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
package io.kyligence.kap.smart;

import java.io.File;
import java.io.Serializable;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.JobRunnerFactory;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProposerJob extends ExecutableApplication implements IKeep {

    public static AbstractContext proposeForAutoMode(KylinConfig config, String project, String[] sqls) {
        AbstractContext context = new SmartContext(config, project, sqls);
        return propose(context);
    }

    public static AbstractContext genOptRec(KylinConfig config, String project, String[] sqls) {
        val context = new ModelReuseContextOfSemiV2(config, project, sqls);
        return propose(context);
    }

    public static AbstractContext selectExistedModel(KylinConfig config, String project, String[] sqls) {
        AbstractContext context = new ModelSelectContextOfSemiV2(config, project, sqls);
        return propose(context);
    }

    public static AbstractContext propose(AbstractContext context) {
        val config = context.getSmartConfig().getKylinConfig();
        val project = context.getProject();
        val sqls = context.getSqlArray();
        val resourceStore = ResourceStore.getKylinMetaStore(config);

        List<String> resources = Lists.newArrayList();
        NDataflowManager.getInstance(config, project).listDataModelsByStatus(RealizationStatusEnum.ONLINE)
                .forEach(model -> {
                    resources.add(model.getResourcePath());
                    resources.add(IndexPlan.concatResourcePath(model.getId(), project));
                });

        Function<Collection<String>, Boolean> orElse = a -> a != null ? resources.addAll(a) : null;
        orElse.apply(resourceStore.listResources(ResourceStore.PROJECT_ROOT));
        orElse.apply(resourceStore.listResources("/" + project + ResourceStore.TABLE_RESOURCE_ROOT));
        orElse.apply(resourceStore.listResources("/" + project + ResourceStore.TABLE_EXD_RESOURCE_ROOT));
        val runner = JobRunnerFactory.createRunner(config, context.getSmartConfig().getProposeRunnerImpl(), project,
                resources);
        runner.setConfigUpdater(
                props -> props.setProperty("kylin.query.queryhistory.url", config.getMetadataUrl().toString()));
        val params = Maps.<String, String> newHashMap();
        params.put("contextClass", context.getClass().getName());

        val jobId = LocalDateTime.now(Clock.systemDefaultZone()).format(
                DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSS", Locale.getDefault(Locale.Category.FORMAT)));
        try {
            val jobTmpDir = runner.prepareEnv(jobId);

            val contextParamsFile = jobTmpDir + "/context_params.json";
            JsonUtil.writeValue(new File(contextParamsFile),
                    new ContextParams(project, context.isCanCreateNewModel(), Lists.newArrayList(sqls)));
            params.put("contextParams", contextParamsFile);

            val contextOutputFile = jobTmpDir + "/context_output.json";
            params.put("contextOutput", contextOutputFile);
            runner.start(new ProposerJob(), params);

            val output = JsonUtil.readValue(new File(contextOutputFile), ContextOutput.class);
            val tables = NTableMetadataManager.getInstance(config, context.getProject()).getAllTablesMap();
            output.getModelContextOutputs().forEach(modelOutput -> {
                val originModel = modelOutput.getOriginModel();
                if (originModel != null) {
                    originModel.init(config, tables, Lists.newArrayList(), context.getProject());
                }
                val targetModel = modelOutput.getTargetModel();
                if (targetModel != null) {
                    targetModel.init(config, tables, Lists.newArrayList(), context.getProject());
                }
                modelOutput.getMeasureRecItemMap().forEach((key, m) -> {
                    m.getMeasure().getFunction().init(targetModel);
                });
            });
            ContextOutput.merge(context, output);
        } catch (Exception e) {
            throw new KylinException(ServerErrorCode.EXEC_JOB_FAILED, "Failed to exec job " + jobId, e);
        } finally {
            runner.cleanupEnv();
        }
        return context;
    }

    static final Option OPTION_META_DIR = OptionBuilder.getInstance().withArgName("meta").hasArg().isRequired(true)
            .withDescription("metadata input directory").create("meta");
    static final Option OPTION_CONTEXT_PARAMS_FILE = OptionBuilder.getInstance().withArgName("contextParams").hasArg()
            .isRequired(true).withDescription("context params file").create("contextParams");
    static final Option OPTION_META_OUTPUT_DIR = OptionBuilder.getInstance().withArgName("metaOutput").hasArg()
            .isRequired(true).withDescription("metadata output directory").create("metaOutput");
    static final Option OPTION_CONTEXT_CLASS = OptionBuilder.getInstance().withArgName("contextClass").hasArg()
            .isRequired(true).withDescription("context implement").create("contextClass");
    static final Option OPTION_CONTEXT_OUTPUT_FILE = OptionBuilder.getInstance().withArgName("contextOutput").hasArg()
            .isRequired(true).withDescription("context output file").create("contextOutput");

    protected final Options options;

    public ProposerJob() {
        options = new Options();
        options.addOption(OPTION_META_DIR);
        options.addOption(OPTION_CONTEXT_PARAMS_FILE);
        options.addOption(OPTION_META_OUTPUT_DIR);
        options.addOption(OPTION_CONTEXT_CLASS);
        options.addOption(OPTION_CONTEXT_OUTPUT_FILE);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        val metaDir = optionsHelper.getOptionValue(OPTION_META_DIR);
        val contextParamsFile = optionsHelper.getOptionValue(OPTION_CONTEXT_PARAMS_FILE);
        val contextOutputFile = optionsHelper.getOptionValue(OPTION_CONTEXT_OUTPUT_FILE);
        val contextClass = optionsHelper.getOptionValue(OPTION_CONTEXT_CLASS);
        val contextParams = JsonUtil.readValue(new File(contextParamsFile), ContextParams.class);
        val sqls = contextParams.getSqls();
        val project = contextParams.getProject();
        try (KylinConfig.SetAndUnsetThreadLocalConfig config = KylinConfig
                .setAndUnsetThreadLocalConfig(KylinConfig.loadKylinConfigFromHdfs(metaDir))) {
            val contextConstructor = Class.forName(contextClass).getConstructor(KylinConfig.class, String.class,
                    String[].class);
            val context = (AbstractContext) contextConstructor.newInstance(KylinConfig.getInstanceFromEnv(), project,
                    sqls.toArray(new String[0]));
            context.setCanCreateNewModel(contextParams.isCanCreateNewModel());
            new SmartMaster(context).runWithContext(null);
            val output = ContextOutput.from(context);
            JsonUtil.writeValue(new File(contextOutputFile), output);
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class ContextParams implements Serializable, IKeep {

        private String project;

        private boolean canCreateNewModel;

        private List<String> sqls = Lists.newArrayList();
    }

    public static void main(String[] args) {
        val tool = new ProposerJob();
        try {
            tool.execute(args);
        } catch (Exception e) {
            log.warn("Propose {} failed", args, e);
            Unsafe.systemExit(1);
        }
        log.info("Propose finished");
        Unsafe.systemExit(0);
    }
}