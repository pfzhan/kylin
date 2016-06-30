package io.kyligence.kap.tool;

import com.google.common.collect.Lists;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.tool.AbstractInfoExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by dongli on 6/11/16.
 */
public class DiagnosisInfoCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosisInfoCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.").create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_HBASE = OptionBuilder.withArgName("includeHBase").hasArg().isRequired(false).withDescription("Specify whether to include hbase files to extract. Default true.").create("includeHBase");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg().isRequired(false).withDescription("Specify whether to include client info to extract. Default true.").create("includeClient");

    public DiagnosisInfoCLI() {
        super();

        packageType = "project";
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_INCLUDE_CONF);
        options.addOption(OPTION_INCLUDE_HBASE);
        options.addOption(OPTION_INCLUDE_CLIENT);
    }

    public static void main(String[] args) {
        args = new String[] { "-destDir", "aa" };
        DiagnosisInfoCLI diagnosisInfoCLI = new DiagnosisInfoCLI();
        diagnosisInfoCLI.execute(args);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        // by default, KAP extracts all project's metadata and hbase usage
        org.apache.kylin.tool.DiagnosisInfoCLI diagnosisInfoCLI = new org.apache.kylin.tool.DiagnosisInfoCLI();
        ArrayList<String> argList = Lists.newArrayList();
        argList.add("-submodule");
        argList.add("true");
        argList.add("-project");
        argList.add("-all");
        argList.add("-destDir");
        argList.add(exportDir.getAbsolutePath());
        if (optionsHelper.hasOption(OPTION_INCLUDE_CONF)) {
            argList.add("-includeConf");
            argList.add(optionsHelper.getOptionValue(OPTION_INCLUDE_CONF));
        }
        if (optionsHelper.hasOption(OPTION_INCLUDE_HBASE)) {
            argList.add("-includeHBase");
            argList.add(optionsHelper.getOptionValue(OPTION_INCLUDE_HBASE));
        }
        if (optionsHelper.hasOption(OPTION_INCLUDE_CLIENT)) {
            argList.add("-includeClient");
            argList.add(optionsHelper.getOptionValue(OPTION_INCLUDE_CLIENT));
        }
        String[] args = new String[argList.size()];
        logger.info(Arrays.toString(args));
        diagnosisInfoCLI.execute(argList.toArray(args));
    }
}
