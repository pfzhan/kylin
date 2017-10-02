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

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampler;
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
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ModelStatsUpdate extends AbstractHadoopJob {
    public static final String JOB_TITLE = "Model Update Job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_MODEL = OptionBuilder.withArgName("model name").hasArg().isRequired(true).withDescription("data model name").create("model");

    private String model;

    private IJoinedFlatTableDesc flatTableDesc;

    private Map<Integer, String> columnIndexMap = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(ModelStatsUpdate.class);

    public ModelStatsUpdate() {

    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        try {
            options.addOption(OPTION_MODEL);
            options.addOption(OPTION_OUTPUT_PATH);

            parseOptions(options, args);

            this.model = getOptionValue(OPTION_MODEL);

            DataModelDesc dataModelDesc = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv()).getDataModelDesc(model);

            flatTableDesc = new DataModelStatsFlatTableDesc(dataModelDesc);

            // start job
            String jobName = JOB_TITLE + getOptionsAsString();
            logger.info("Starting: " + jobName);
            Configuration conf = getConf();
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            updateModelStats(model, output.toString(), conf);
            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public void updateModelStats(String model, String outPath, Configuration config) throws IOException {
        TreeMap<Integer, HiveTableExtSampler> samplers = null;

        samplers = read(new Path(outPath), config);

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(kylinConfig);
        ModelStats modelStats = modelStatsManager.getModelStats(model);
        Map<String, Long> singleCardMap = new HashMap<>();
        Map<String, Long> combinationCardMap = new HashMap<>();
        Map<String, Long> columnNullMap = new HashMap<>();
        boolean once = true;
        for (Map.Entry<Integer, HiveTableExtSampler> sampler : samplers.entrySet()) {
            singleCardMap.put(columnIndexMap.get(sampler.getKey()), sampler.getValue().getCardinality());
            combinationCardMap.putAll(convertCombinationCardMap(sampler.getValue().getCombinationCardinality()));
            columnNullMap.put(sampler.getValue().getColumnName(), Long.parseLong(sampler.getValue().getNullCounter()));

            if (once) {
                modelStats.setCounter(Long.parseLong(sampler.getValue().getCounter()));
                modelStats.setFrequency(sampler.getValue().getStatsSampleFrequency());
                once = false;
            }
            sampler.getValue().clean();
        }
        modelStats.setColumnNullMap(columnNullMap);
        modelStats.setDoubleColumnCardinality(combinationCardMap);
        modelStats.setSingleColumnCardinality(singleCardMap);
        modelStatsManager.saveModelStats(modelStats);
        ModelDiagnose.checkJointResult(flatTableDesc.getDataModel(), modelStats, kylinConfig);
    }

    private Map<String, Long> convertCombinationCardMap(Map<String, Long> combinationCardMap) {
        Map<String, Long> convertedComCardMap = new HashMap<>();
        for (Map.Entry<String, Long> combinationCard : combinationCardMap.entrySet()) {
            String[] keys = combinationCard.getKey().split(",");
            if (keys.length != 2)
                throw new IllegalArgumentException("Illegal combination key in Model stats" + model);
            StringBuilder keyBuilder = new StringBuilder();
            keyBuilder.append(columnIndexMap.get(Integer.parseInt(keys[0])));
            keyBuilder.append(",");
            keyBuilder.append(columnIndexMap.get(Integer.parseInt(keys[1])));
            convertedComCardMap.put(keyBuilder.toString(), combinationCard.getValue());
        }
        return convertedComCardMap;
    }

    private TreeMap<Integer, HiveTableExtSampler> read(Path path, Configuration conf) throws IOException {
        TreeMap<Integer, HiveTableExtSampler> samplers = new TreeMap<>();
        List<TblColRef> columns = flatTableDesc.getAllColumns();
        for (Path p : getAllPaths(path, conf)) {
            SequenceFile.Reader.Option seqInput = SequenceFile.Reader.file(p);
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, seqInput);

            IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                String type = columns.get(key.get()).getType().getName();
                int precision = columns.get(key.get()).getType().getPrecision();
                HiveTableExtSampler sampler = new HiveTableExtSampler(type, precision, key.get(), columns.size());
                sampler.decode(ByteBuffer.wrap(value.getBytes()));
                columnIndexMap.put(key.get(), sampler.getColumnName());
                samplers.put(key.get(), sampler);
            }
        }
        return samplers;
    }

    private static List<Path> getAllPaths(Path root, Configuration conf) throws IOException {
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem(conf);
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
