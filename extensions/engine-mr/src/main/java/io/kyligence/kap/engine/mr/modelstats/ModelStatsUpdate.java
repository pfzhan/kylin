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

package io.kyligence.kap.engine.mr.modelstats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.tablestats.HiveTableExtSampler;

public class ModelStatsUpdate extends AbstractHadoopJob {
    public static final String JOB_TITLE = "Model Update Job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_TABLE = OptionBuilder.withArgName("table name").hasArg().isRequired(true).withDescription("The hive table name").create("table");

    private String table;

    private static final Logger logger = LoggerFactory.getLogger(ModelStatsUpdate.class);

    public ModelStatsUpdate() {

    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        try {
            options.addOption(OPTION_TABLE);
            options.addOption(OPTION_OUTPUT_PATH);

            parseOptions(options, args);

            this.table = getOptionValue(OPTION_TABLE).toUpperCase();
            // start job
            String jobName = JOB_TITLE + getOptionsAsString();
            logger.info("Starting: " + jobName);
            Configuration conf = getConf();
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            updateTableSample(table.toUpperCase(), output.toString(), conf);
            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public void updateTableSample(String tableName, String outPath, Configuration config) throws IOException {
        TreeMap<Integer, HiveTableExtSampler> samplers = null;
        try {
            samplers = read(new Path(outPath), config);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to resolve samples for " + tableName + " from " + outPath);
            return;
        }

        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(KylinConfig.getInstanceFromEnv());
        ModelStats modelStats = modelStatsManager.getModelStats(tableName);
        Map<Integer, Long> singleCardMap = new HashMap<>();

        for (Map.Entry<Integer, HiveTableExtSampler> sampler : samplers.entrySet()) {
            singleCardMap.put(sampler.getKey(), sampler.getValue().getCardinality());
            modelStats.appendDoubleColumnCardinality(sampler.getValue().getCombinationCardinality());
            sampler.getValue().clean();
        }
        modelStats.setSingleColumnCardinality(singleCardMap);
        modelStatsManager.saveModelStats(modelStats);
    }

    private static TreeMap<Integer, HiveTableExtSampler> read(Path path, Configuration conf) throws IOException {
        TreeMap<Integer, HiveTableExtSampler> samplers = new TreeMap<>();
        for (Path p : getAllPaths(path, conf)) {
            SequenceFile.Reader.Option seqInput = SequenceFile.Reader.file(p);
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, seqInput);

            IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                HiveTableExtSampler sampler = new HiveTableExtSampler();
                sampler.decode(ByteBuffer.wrap(value.getBytes()));
                samplers.put(key.get(), sampler);
            }
        }
        return samplers;
    }

    private static List<Path> getAllPaths(Path root, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(root.toUri(), conf);
        FileStatus[] items = fileSystem.listStatus(root);
        if (items == null)
            return new ArrayList<>();

        List<Path> results = new ArrayList<>();

        for (FileStatus item : items) {

            // ignoring files like _SUCCESS
            if (item.getPath().getName().startsWith("_")) {
                continue;
            }
            results.add(item.getPath());
        }
        return results;
    }
}
