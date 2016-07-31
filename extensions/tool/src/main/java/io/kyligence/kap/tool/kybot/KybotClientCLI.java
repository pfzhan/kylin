/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.tool.kybot;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.tool.AbstractInfoExtractor;
import org.apache.kylin.tool.DiagnosisInfoCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 6/11/16.
 */
public class KybotClientCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(KybotClientCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.").create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_HBASE = OptionBuilder.withArgName("includeHBase").hasArg().isRequired(false).withDescription("Specify whether to include hbase files to extract. Default true.").create("includeHBase");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg().isRequired(false).withDescription("Specify whether to include client info to extract. Default true.").create("includeClient");

    public KybotClientCLI() {
        super();

        packageType = "project";
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_INCLUDE_CONF);
        options.addOption(OPTION_INCLUDE_HBASE);
        options.addOption(OPTION_INCLUDE_CLIENT);
    }

    public static void main(String[] args) {
        KybotClientCLI kybotClientCLI = new KybotClientCLI();
        kybotClientCLI.execute(args);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        // by default, KAP extracts all project's metadata and hbase usage
        DiagnosisInfoCLI diagnosisInfoCLI = new DiagnosisInfoCLI();
        ArrayList<String> argList = Lists.newArrayList();
        argList.add("-submodule");
        argList.add("true");
        argList.add("-compress");
        argList.add("false");
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
        logger.info(StringUtils.join(argList, ","));
        diagnosisInfoCLI.execute(argList.toArray(args));

        String[] garbageArgs = { "-destDir", new File(exportDir, "garbage").getAbsolutePath(), "-compress", "false", "-submodule", "true" };
        StorageGarbageStatsExtractor garbageExtractor = new StorageGarbageStatsExtractor();
        logger.info("StorageGarbageStatsExtractor args: " + Arrays.toString(garbageArgs));
        garbageExtractor.execute(garbageArgs);
    }
}
