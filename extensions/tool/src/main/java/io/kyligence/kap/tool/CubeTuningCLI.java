package io.kyligence.kap.tool;

import com.google.common.collect.Lists;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.tool.AbstractInfoExtractor;
import org.apache.kylin.tool.ClientEnvExtractor;
import org.apache.kylin.tool.CubeMetaExtractor;
import org.apache.kylin.tool.KylinLogExtractor;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dongli on 6/11/16.
 */
public class CubeTuningCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(CubeTuningCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.").create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_MODEL = OptionBuilder.withArgName("model").hasArg().isRequired(true).withDescription("Specify which model to extract").create("model");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg().isRequired(false).withDescription("Specify whether to include client info to extract. Default true.").create("includeClient");

    @SuppressWarnings("static-access")
    private static final Option OPTION_SAMPLE_RATE = OptionBuilder.withArgName("sampleRate").hasArg().isRequired(false).withDescription("Specify sampling rate.").create("sampleRate");

    public CubeTuningCLI() {
        super();
        packageType = "project";
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_INCLUDE_CONF);
        options.addOption(OPTION_INCLUDE_CLIENT);
        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_SAMPLE_RATE);
    }

    public static void main(String args[]) {
        args = new String[]{"-destDir", "bb","-model","all","-project","all"};
        CubeTuningCLI diagnosisInfoCLI = new CubeTuningCLI();
        diagnosisInfoCLI.execute(args);
    }

    private List<String> getProjects(String projectSeed) {
        List<String> result = Lists.newLinkedList();
        if (projectSeed.equalsIgnoreCase("all")) {
            ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
                result.add(projectInstance.getName());
            }
        } else {
            result.add(projectSeed);
        }
        return result;
    }


    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final String projectInput = optionsHelper.getOptionValue(options.getOption("project"));
        final String model = optionsHelper.getOptionValue(options.getOption("model"));
        String sampleRate = optionsHelper.hasOption(OPTION_SAMPLE_RATE) ? optionsHelper.getOptionValue(OPTION_SAMPLE_RATE) : "10";
        boolean includeConf = optionsHelper.hasOption(OPTION_INCLUDE_CONF) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CONF)) : true;
        boolean includeClient = optionsHelper.hasOption(OPTION_INCLUDE_CLIENT) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CLIENT)) : true;

        for (String project : getProjects(projectInput)) {
            // export cube metadata
            String[] cubeMetaArgs = {"-destDir", new File(exportDir, "metadata").getAbsolutePath(), "-project", project, "-compress", "false", "-submodule", "true"};
            CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
            logger.info("CubeMetaExtractor args: " + Arrays.toString(cubeMetaArgs));
            cubeMetaExtractor.execute(cubeMetaArgs);

            // export conf
            if (includeConf) {
                logger.info("Start to extract kylin conf files.");
                try {
                    FileUtils.copyDirectoryToDirectory(new File(ToolUtil.getConfFolder()), exportDir);
                } catch (Exception e) {
                    logger.warn("Error in export conf.", e);
                }
            }

            // export client
            if (includeClient) {
                String[] clientArgs = {"-destDir", new File(exportDir, "client").getAbsolutePath(), "-compress", "false", "-submodule", "true"};
                ClientEnvExtractor clientEnvExtractor = new ClientEnvExtractor();
                logger.info("ClientEnvExtractor args: " + Arrays.toString(clientArgs));
                clientEnvExtractor.execute(clientArgs);
            }

            // export logs
            String[] logsArgs = {"-destDir", new File(exportDir, "logs").getAbsolutePath(), "-compress", "false", "-submodule", "true"};
            KylinLogExtractor logExtractor = new KylinLogExtractor();
            logger.info("KylinLogExtractor args: " + Arrays.toString(logsArgs));
            logExtractor.execute(logsArgs);

            // export sample data
            String[] sampleDataArgs = {"-destDir", new File(exportDir, "sample").getAbsolutePath(), "-compress", "false", "-submodule", "true", "-model", model,"-sampleRate",sampleRate};
            //todo:extract all models
            SampleDataExtractor sampleDataExtractor = new SampleDataExtractor();
            logger.info("SampleDataExtractor args: " + Arrays.toString(sampleDataArgs));
            sampleDataExtractor.execute(sampleDataArgs);
        }
    }
}
