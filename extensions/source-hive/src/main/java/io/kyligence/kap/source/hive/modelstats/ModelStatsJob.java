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

package io.kyligence.kap.source.hive.modelstats;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.hive.HiveMRInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;

public class ModelStatsJob extends AbstractHadoopJob {
    private static final Logger logger = LoggerFactory.getLogger(ModelStatsJob.class);

    public static final String JOB_TITLE = "KAP DataModel stats job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_MODEL = OptionBuilder.withArgName("model name").hasArg().isRequired(true)
            .withDescription("data model name").create("model");
    protected static final Option OPTION_FREQUENCY = OptionBuilder.withArgName("sample frequency").hasArg()
            .isRequired(true).withDescription("The sample frequency").create("frequency");
    protected static final Option OPTION_JOB_ID = OptionBuilder.withArgName("job id").hasArg().isRequired(true)
            .withDescription("job id").create("jobId");

    public ModelStatsJob() {
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_FREQUENCY);
        options.addOption(OPTION_JOB_ID);

        parseOptions(options, args);

        // start job
        String jobName = JOB_TITLE + getOptionsAsString();
        logger.info("Starting: " + jobName);
        Configuration conf = getConf();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        JobEngineConfig jobEngineConfig = new JobEngineConfig(kylinConfig);
        conf.addResource(new Path(jobEngineConfig.getHadoopJobConfFilePath(null)));

        String model = getOptionValue(OPTION_MODEL);
        String jobId = getOptionValue(OPTION_JOB_ID);
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc(model);

        IJoinedFlatTableDesc flatTableDesc = new DataModelStatsFlatTableDesc(modelDesc, null, jobId);

        job = Job.getInstance(conf, jobName);

        setJobClasspath(job, kylinConfig);

        Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.CFG_TABLE_NAME, model);
        job.getConfiguration().set(BatchConstants.CFG_STATS_JOB_ID, jobId);
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress", "false");
        job.getConfiguration().set(BatchConstants.CFG_STATS_JOB_FREQUENCY, getOptionValue(OPTION_FREQUENCY));
        // Mapper
        String fullTableName = kylinConfig.getHiveDatabaseForIntermediateTable() + "." + flatTableDesc.getTableName();
        IMRInput.IMRTableInputFormat tableInputFormat = new HiveMRInput.HiveTableInputFormat(fullTableName);
        tableInputFormat.configureJob(job);

        job.setMapperClass(ModelStatsMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setReducerClass(ModelStatsReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setNumReduceTasks(decideReduceTasks(flatTableDesc.getAllColumns().size()));

        this.deletePath(job.getConfiguration(), output);

        logger.info("Going to submit Model stats Job for model '" + model + "'");

        attachKylinPropsAndMetadata(modelDesc, job.getConfiguration());
        int result = waitForCompletion(job);

        return result;
    }

    protected void attachKylinPropsAndMetadata(DataModelDesc desc, Configuration conf) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.add(desc.getResourcePath());

        for (TableRef tableRef : desc.getAllTables()) {
            TableDesc table = tableRef.getTableDesc();
            dumpList.add(table.getResourcePath());
            List<String> dependentResources = SourceFactory.getMRDependentResources(table);
            dumpList.addAll(dependentResources);
        }

        dumpKylinPropsAndMetadata(desc.getProject(), dumpList, KylinConfig.getInstanceFromEnv(), conf);
    }

    private int decideReduceTasks(int columnCount) {
        if (columnCount > 30) {
            return 30;
        }
        return columnCount;
    }
}
