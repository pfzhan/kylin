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

package io.kyligence.kap.storage.parquet.cube.spark;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverServer;

public class SparkQueryDriver extends AbstractApplication {

    protected static final Logger logger = LoggerFactory.getLogger(SparkQueryDriver.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_PORT = OptionBuilder.withArgName("port").hasArg().isRequired(true).withDescription("specify which port to listen to").create("port");

    private Options options;

    public SparkQueryDriver() {
        options = new Options();
        options.addOption(OPTION_PORT);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {

        SparkDriverServer server = new SparkDriverServer(Integer.valueOf(optionsHelper.getOptionValue(OPTION_PORT)));
        server.start();
        server.blockUntilShutdown();
    }
}
