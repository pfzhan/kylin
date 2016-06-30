package io.kyligence.kap.tool;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.tool.AbstractInfoExtractor;
import org.apache.kylin.tool.CubeMetaExtractor;
import org.apache.kylin.tool.KylinLogExtractor;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Created by dongli on 6/11/16.
 */
public class CubeTuningCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(CubeTuningCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify models in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_MODEL = OptionBuilder.withArgName("model").hasArg().isRequired(true).withDescription("Specify which model to extract").create("model");

    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(true).withDescription("Specify model related to which cube to extract").create("cube");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false).withDescription("Specify whether to include conf files to extract. Default false.").create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_SAMPLE_RATE = OptionBuilder.withArgName("sampleRate").hasArg().isRequired(false).withDescription("Specify sampling rate.").create("sampleRate");

    KylinConfig kylinConfig;
    MetadataManager metadataManager;
    CubeDescManager cubeDescManager;

    public CubeTuningCLI() {
        super();

        packageType = "cubeTunings";

        OptionGroup modelFilterOption = new OptionGroup();
        modelFilterOption.addOption(OPTION_CUBE);
        modelFilterOption.addOption(OPTION_PROJECT);
        modelFilterOption.addOption(OPTION_MODEL);

        options.addOptionGroup(modelFilterOption);
        options.addOption(OPTION_INCLUDE_CONF);
        options.addOption(OPTION_SAMPLE_RATE);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(kylinConfig);
        cubeDescManager = CubeDescManager.getInstance(kylinConfig);
    }

    public static void main(String[] args) {
        String[] args1 = new String[] { "-destDir", "cubeTuning" };

        CubeTuningCLI cubeTuningCLI = new CubeTuningCLI();
        cubeTuningCLI.execute(args1);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        boolean includeConf = optionsHelper.hasOption(OPTION_INCLUDE_CONF) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CONF)) : false;
        String sampleRate = optionsHelper.hasOption(OPTION_SAMPLE_RATE) ? optionsHelper.getOptionValue(OPTION_SAMPLE_RATE) : "10";
        Set<String> modelNames = Sets.newHashSet();

        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            String projectName = optionsHelper.getOptionValue(OPTION_PROJECT);
            for (DataModelDesc modelDesc : metadataManager.getModels(projectName)) {
                modelNames.add(modelDesc.getName());
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
            CubeDesc cubeDesc = cubeDescManager.getCubeDesc(cubeName);
            modelNames.add(cubeDesc.getModelName());
        } else if (optionsHelper.hasOption(OPTION_MODEL)) {
            String modelName = optionsHelper.getOptionValue(OPTION_MODEL);
            modelNames.add(modelName);
        } else {
            for (DataModelDesc modelDesc : metadataManager.getModels()) {
                modelNames.add(modelDesc.getName());
            }
        }

        // export sample data
        String modelSeed = StringUtils.join(modelNames, ",");
        String[] sampleDataArgs = { "-destDir", new File(exportDir, "data").getAbsolutePath(), "-compress", "false", "-submodule", "true", "-model", modelSeed, "-sampleRate", sampleRate };
        SampleDataExtractor sampleDataExtractor = new SampleDataExtractor();
        logger.info("SampleDataExtractor args: " + Arrays.toString(sampleDataArgs));
        sampleDataExtractor.execute(sampleDataArgs);

        // export cube metadata
        for (ProjectInstance project : ProjectManager.getInstance(kylinConfig).listAllProjects()) {
            String[] cubeMetaArgs = { "-destDir", new File(exportDir, "metadata").getAbsolutePath(), "-project", project.getName(), "-compress", "false", "-submodule", "true" };
            CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
            logger.info("CubeMetaExtractor args: " + Arrays.toString(cubeMetaArgs));
            cubeMetaExtractor.execute(cubeMetaArgs);
        }

        // export conf
        if (includeConf) {
            logger.info("Start to extract kylin conf files.");
            try {
                FileUtils.copyDirectoryToDirectory(new File(ToolUtil.getConfFolder()), exportDir);
            } catch (Exception e) {
                logger.warn("Error in export conf.", e);
            }
        }

        // export logs
        String[] logsArgs = { "-destDir", new File(exportDir, "logs").getAbsolutePath(), "-compress", "false", "-submodule", "true" };
        KylinLogExtractor logExtractor = new KylinLogExtractor();
        logger.info("KylinLogExtractor args: " + Arrays.toString(logsArgs));
        logExtractor.execute(logsArgs);

    }
}
