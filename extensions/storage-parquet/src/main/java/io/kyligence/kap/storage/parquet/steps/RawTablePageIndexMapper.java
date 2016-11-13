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

package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;
import io.kyligence.kap.storage.parquet.format.raw.RawTableUtils;

public class RawTablePageIndexMapper extends KylinMapper<ByteArrayListWritable, IntWritable, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(RawTablePageIndexMapper.class);

    protected String cubeName;
    protected String shardId;
    protected RawTableInstance rawTableInstance;
    protected RawTableDesc rawTableDesc;

    private KapConfig kapConfig;
    private Path inputPath;
    private ParquetPageIndexWriter indexBundleWriter;
    private int counter = 0;
    private Path outputPath;
    private Path tmpPath;

    private ColumnSpec[] columnSpecs;

    private double spillThresholdMB;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        inputPath = ((FileSplit) context.getInputSplit()).getPath();

        super.bindCurrentConfiguration(conf);

        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
        kapConfig = KapConfig.wrap(AbstractHadoopJob.loadKylinPropsAndMetadata());
        spillThresholdMB = kapConfig.getParquetPageIndexSpillThresholdMB();

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));

        // write to same dir with input
        outputPath = new Path(inputPath.getParent(), shardId + ".parquet.inv");
        rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(cubeName);
        rawTableDesc = rawTableInstance.getRawTableDesc();

        logger.info("Input path: " + inputPath.toUri().toString());
        logger.info("Output path: " + outputPath.toString());

        initIndexWriters(context);
    }

    private void initIndexWriters(Context context) throws IOException, InterruptedException {
        int hashLength = KapConfig.getInstanceFromEnv().getParquetIndexHashLength();

        List<TblColRef> columns = rawTableDesc.getColumns();
        columnSpecs = new ColumnSpec[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            columnSpecs[i] = new ColumnSpec(column.getName(), hashLength, 10000, true, i);
            columnSpecs[i].setValueEncodingIdentifier('s');
        }

        tmpPath = new Path(outputPath.getParent(), String.valueOf(shardId) + "-" + RandomStringUtils.randomAlphabetic(10) + ".tmp");

        FSDataOutputStream outputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).create(tmpPath);
        indexBundleWriter = new ParquetPageIndexWriter(columnSpecs, outputStream);
    }

    @Override
    public void doMap(ByteArrayListWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }

        List<byte[]> originValue = key.get();
        List<byte[]> hashedValue = RawTableUtils.hash(originValue);

        indexBundleWriter.write(hashedValue, value.get());

        spillIfNeeded();
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        indexBundleWriter.spill();
        indexBundleWriter.close();

        FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
        fs.mkdirs(outputPath.getParent());
        fs.rename(tmpPath, outputPath);
        logger.info("move file {} to {}", tmpPath, outputPath);
    }

    private void spillIfNeeded() {
        long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
        if (availMemoryMB < spillThresholdMB) {
            logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
            indexBundleWriter.spill();
            logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
        }
    }
}
