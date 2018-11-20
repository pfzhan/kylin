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

package io.kyligence.kap.engine.spark.builder;

import io.kyligence.kap.engine.spark.job.UdfManager;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.NBatchConstants;

public class NDataflowJob extends AbstractApplication {
    protected static final Logger logger = LoggerFactory.getLogger(NDataflowJob.class);

    @SuppressWarnings("static-access")
    public static final Option OPTION_DATAFLOW_NAME = OptionBuilder.withArgName(NBatchConstants.P_DATAFLOW_NAME)
            .hasArg().isRequired(true).withDescription("DataFlow Name").create(NBatchConstants.P_DATAFLOW_NAME);
    @SuppressWarnings("static-access")
    public static final Option OPTION_PROJECT_NAME = OptionBuilder.withArgName(NBatchConstants.P_PROJECT_NAME).hasArg()
            .isRequired(true).withDescription("DataFlow Name").create(NBatchConstants.P_PROJECT_NAME);
    @SuppressWarnings("static-access")
    public static final Option OPTION_SEGMENT_IDS = OptionBuilder.withArgName(NBatchConstants.P_SEGMENT_IDS).hasArg()
            .isRequired(true).withDescription("Segment indices").create(NBatchConstants.P_SEGMENT_IDS);
    @SuppressWarnings("static-access")
    public static final Option OPTION_LAYOUT_IDS = OptionBuilder.withArgName(NBatchConstants.P_CUBOID_LAYOUT_IDS)
            .hasArg().isRequired(true).withDescription("Layout indices").create(NBatchConstants.P_CUBOID_LAYOUT_IDS);
    @SuppressWarnings("static-access")
    public static final Option OPTION_META_URL = OptionBuilder.withArgName(NBatchConstants.P_DIST_META_URL).hasArg()
            .isRequired(true).withDescription("Cubing metadata url").create(NBatchConstants.P_DIST_META_URL);

    @SuppressWarnings("static-access")
    public static final Option OPTION_JOB_ID = OptionBuilder.withArgName(NBatchConstants.P_JOB_ID).hasArg()
            .isRequired(true).withDescription("Current job id").create(NBatchConstants.P_JOB_ID);

    protected volatile KylinConfig config;
    protected volatile String jobId;
    protected SparkSession ss;
    protected String project;

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DATAFLOW_NAME);
        options.addOption(OPTION_PROJECT_NAME);
        options.addOption(OPTION_SEGMENT_IDS);
        options.addOption(OPTION_LAYOUT_IDS);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_JOB_ID);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String hdfsMetalUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);
        ss = SparkSession.builder().enableHiveSupport().getOrCreate();
        //#8341
        SparderEnv.setSparkSession(ss);
        UdfManager.create(ss);
        config = KylinConfig.loadKylinConfigFromHdfs(hdfsMetalUrl);
        KylinConfig.setKylinConfigThreadLocal(config);
    }
}
