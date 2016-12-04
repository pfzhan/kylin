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

package io.kyligence.kap.engine.mr.tablestats;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;

public class HiveTableExtJob extends AbstractHadoopJob {
    public static final String JOB_TITLE = "Kylin Hive Column Sample Job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_TABLE = OptionBuilder.withArgName("table name").hasArg().isRequired(true).withDescription("The hive table name").create("table");

    public HiveTableExtJob() {
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        options.addOption(OPTION_TABLE);
        options.addOption(OPTION_OUTPUT_PATH);

        parseOptions(options, args);

        // start job
        String jobName = JOB_TITLE + getOptionsAsString();
        logger.info("Starting: " + jobName);
        Configuration conf = getConf();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        JobEngineConfig jobEngineConfig = new JobEngineConfig(kylinConfig);
        conf.addResource(new Path(jobEngineConfig.getHadoopJobConfFilePath(null)));

        String table = getOptionValue(OPTION_TABLE);
        TableDesc tableDesc = MetadataManager.getInstance(kylinConfig).getTableDesc(table);

        job = Job.getInstance(conf, jobName);

        setJobClasspath(job, kylinConfig);

        job.getConfiguration().set(BatchConstants.CFG_TABLE_NAME, table);

        Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set("dfs.block.size", "67108864");

        // Mapper
        IMRInput.IMRTableInputFormat tableInputFormat = MRUtil.getTableInputFormat(table);
        tableInputFormat.configureJob(job);

        job.setMapperClass(HiveTableExtMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // Reducer - only one
        job.setReducerClass(HiveTableExtReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(decideReduceTasks(tableDesc.getColumnCount()));

        this.deletePath(job.getConfiguration(), output);

        logger.info("Going to submit Hive Sample Job for table '" + table + "'");

        attachKylinPropsAndMetadata(tableDesc, job.getConfiguration());
        int result = waitForCompletion(job);

        return result;
    }

    private int decideReduceTasks(int columnCount) {
        return 1;
        /*
        if (columnCount > 20) {
            return (int) Math.sqrt(columnCount);
        }
        return columnCount;
        */
    }

}
