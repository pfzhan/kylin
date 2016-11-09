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
import java.nio.ByteBuffer;
import java.util.HashMap;
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
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.BufferedRawColumnCodec;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;
import io.kyligence.kap.cube.raw.gridtable.RawTableGridTable;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;
import io.kyligence.kap.storage.parquet.format.raw.RawTableUtils;

public class RawTableFuzzyIndexMapper extends KylinMapper<ByteArrayListWritable, IntWritable, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(RawTableFuzzyIndexMapper.class);

    protected String cubeName;
    protected String shardId;
    protected RawTableInstance rawTableInstance;
    protected RawTableDesc rawTableDesc;
    private BufferedRawColumnCodec rawColumnCodec;
    private String[] codecBuffer;

    private Path inputPath;
    private HashMap<Integer, ParquetPageIndexWriter> fuzzyIndexWriterMap;
    private HashMap<Integer, ImmutableBitSet> fuzzyIndexEncodingMap;
    private int counter = 0;
    private int fuzzyLength = 0;
    private int fuzzyHashLength = 0;
    private Path outputPath;

    private HashMap<Path, Path> pathMap;

    private double spillThresholdMB;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        inputPath = ((FileSplit) context.getInputSplit()).getPath();
        pathMap = new HashMap<>();

        super.bindCurrentConfiguration(conf);

        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
        KapConfig kapConfig = KapConfig.wrap(AbstractHadoopJob.loadKylinPropsAndMetadata());
        fuzzyLength = kapConfig.getParquetFuzzyIndexLength();
        fuzzyHashLength = kapConfig.getParquetFuzzyIndexHashLength();
        spillThresholdMB = kapConfig.getParquetPageIndexSpillThresholdMB();

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));

        // write to same dir with input
        outputPath = new Path(inputPath.getParent(), shardId + ".parquet.inv");

        rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(cubeName);
        rawTableDesc = rawTableInstance.getRawTableDesc();
        GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableInstance);
        rawColumnCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());
        codecBuffer = new String[rawColumnCodec.getColumnsCount()];//a little bit waste

        logger.info("Input path: " + inputPath.toUri().toString());
        logger.info("Output path: " + outputPath.toString());

        initIndexWriters(context);
    }

    private void initIndexWriters(Context context) throws IOException, InterruptedException {
        fuzzyIndexWriterMap = new HashMap<>();
        fuzzyIndexEncodingMap = new HashMap<>();
        List<TblColRef> columns = rawTableDesc.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            if (rawTableDesc.isNeedFuzzyIndex(column)) {
                Path outputPath = new Path(inputPath.getParent(), shardId + "." + i + ".parquet.fuzzy");
                Path tmpPath = new Path(outputPath.getParent(), String.valueOf(shardId) + "-" + String.valueOf(i) + "-" + RandomStringUtils.random(10) + ".tmp");
                pathMap.put(outputPath, tmpPath);

                FSDataOutputStream output = FileSystem.get(HadoopUtil.getCurrentConfiguration()).create(tmpPath);
                ColumnSpec columnSpec = new ColumnSpec(column.getName(), RawTableUtils.roundToByte(fuzzyHashLength), 10000, true, i);
                columnSpec.setValueEncodingIdentifier('s');
                fuzzyIndexWriterMap.put(i, new ParquetPageIndexWriter(new ColumnSpec[] { columnSpec }, output));
                fuzzyIndexEncodingMap.put(i, new ImmutableBitSet(rawTableInstance.getRawToGridTableMapping().getIndexOf(column)));
            }
        }
    }

    @Override
    public void doMap(ByteArrayListWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }

        List<byte[]> originValue = key.get();

        for (Integer fuzzyIndex : fuzzyIndexWriterMap.keySet()) {
            ParquetPageIndexWriter writer = fuzzyIndexWriterMap.get(fuzzyIndex);
            ImmutableBitSet cols = fuzzyIndexEncodingMap.get(fuzzyIndex);// only contain one element
            rawColumnCodec.decode(ByteBuffer.wrap(originValue.get(fuzzyIndex)), codecBuffer, cols);
            if (codecBuffer[cols.trueBitAt(0)] != null) {
                writeSubstring(writer, codecBuffer[cols.trueBitAt(0)].toLowerCase().getBytes(), value.get(), fuzzyLength);
            }
        }

        spillIfNeeded();
    }

    private void writeSubstring(ParquetPageIndexWriter writer, byte[] value, int pageId, int length) {
        // skip if value's length is less than required length
        if (value.length < length) {
            return;
        }

        for (int index = 0; index <= (value.length - length); index++) {
            writer.write(RawTableUtils.shrink(value, index, length, fuzzyHashLength), 0, pageId);
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        for (ParquetPageIndexWriter writer : fuzzyIndexWriterMap.values()) {
            writer.spill();
            writer.close();
        }

        FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
        for (Path outputPath : pathMap.keySet()) {
            fs.mkdirs(outputPath.getParent());
            fs.rename(pathMap.get(outputPath), outputPath);
            logger.info("move file {} to {}", pathMap.get(outputPath), outputPath);
        }
    }

    private void spillIfNeeded() {
        long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
        if (availMemoryMB < spillThresholdMB) {
            logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
            for (ParquetPageIndexWriter writer : fuzzyIndexWriterMap.values()) {
                writer.spill();
            }
            logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
        }
    }
}
