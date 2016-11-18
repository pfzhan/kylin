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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableExtUpdate extends AbstractHadoopJob {
    public static final String JOB_TITLE = "Hive Sample Update Job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_TABLE = OptionBuilder.withArgName("table name").hasArg().isRequired(true).withDescription("The hive table name").create("table");

    private String table;

    private static final Logger logger = LoggerFactory.getLogger(HiveTableExtUpdate.class);

    public HiveTableExtUpdate() {

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
        List<String> columns = null;
        try {
            columns = readLines(new Path(outPath), config);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to resolve samples for " + tableName + " from " + outPath);
            return;
        }

        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        TableExtDesc tableSample = metaMgr.getTableExt(tableName);
        List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>();
        List<String[]> sampleRows = new ArrayList<>();
        Iterator<String> it = columns.iterator();
        HiveTableExtSampler sampler = new HiveTableExtSampler();
        String counter = "0";
        while (it.hasNext()) {
            String string = (String) it.next();
            sampler.parseHdfsToSamples(string);
            TableExtDesc.ColumnStats columnStats = new TableExtDesc.ColumnStats();
            columnStats.setColumnSamples(sampler.getMax(), sampler.getMin(), sampler.getMaxLenValue(), sampler.getMinLenValue());
            sampleRows.add(sampler.getRawSampleValues());
            columnStatsList.add(columnStats);
            counter = sampler.getCounter();
        }
        tableSample.setColumnStats(columnStatsList);
        tableSample.setSampleRows(sampleRows);
        tableSample.setTotalRows(counter);
        metaMgr.saveTableExt(tableSample);
    }

    private static List<String> readLines(Path location, Configuration conf) throws Exception {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);
        if (items == null)
            return new ArrayList<String>();
        List<String> results = new ArrayList<String>();
        for (FileStatus item : items) {

            // ignoring files like _SUCCESS
            if (item.getPath().getName().startsWith("_")) {
                continue;
            }

            CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            } else {
                stream = fileSystem.open(item.getPath());
            }

            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            String raw = writer.toString();
            for (String str : raw.split("\n")) {
                results.add(str);
            }
        }
        return results;
    }

}