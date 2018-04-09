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

package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.BufferedRawColumnCodec;
import io.kyligence.kap.cube.raw.RawTableColumnFamilyDesc;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;
import io.kyligence.kap.cube.raw.gridtable.RawTableGridTable;
import io.kyligence.kap.cube.raw.gridtable.RawToGridTableMapping;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;

public class ParquetRawTableInputFormat extends FileInputFormat<ByteArrayListWritable, ByteArrayListWritable> {
    public org.apache.hadoop.mapreduce.RecordReader<ByteArrayListWritable, ByteArrayListWritable> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetRawTableReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class ParquetRawTableReader extends RecordReader<ByteArrayListWritable, ByteArrayListWritable> {
        protected static final Logger logger = LoggerFactory.getLogger(ParquetRawTableReader.class);

        protected Configuration conf;
        private KylinConfig kylinConfig;
        private RawTableInstance rawTableInstance;
        private RawTableDesc rawTableDesc;
        private BufferedRawColumnCodec rawColumnsCodec;
        private RawTableColumnFamilyDesc[] cfDescs;
        private RawToGridTableMapping mapping;
        
        private Path shardPath;
        private ParquetBundleReader reader = null;

        private ByteArrayListWritable key;
        private ByteArrayListWritable val = new ByteArrayListWritable();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            shardPath = fileSplit.getPath();

            logger.info("shard file path: {}", shardPath.toString());
            
            kylinConfig = KylinConfig.loadKylinPropsAndMetadata();
            String rawName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            logger.info("RawTableName is " + rawName);
            rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(rawName);
            rawTableDesc = rawTableInstance.getRawTableDesc();
            GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableDesc);
            rawColumnsCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());
            cfDescs = rawTableDesc.getRawTableMapping().getColumnFamily();
            mapping = rawTableDesc.getRawToGridTableMapping();

            // init with first shard file
            reader = new ParquetBundleReader.Builder().setConf(conf).setPath(shardPath).build();
        }

        /**
         * All columns are put in key, value is empty
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            
            // Step 1: transform data object list to byte array list of column family. 
            List<Object> row = reader.read();
            if (row == null) {
                return false;
            }
            List<byte[]> newRow = Lists.transform(row, new Function<Object, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(@Nullable Object input) {
                    return ((Binary) input).getBytes();
                }
            });

            // Step 2: transform byte array list of column family to byte array. 
            List<byte[]> newRowForColumn = new ArrayList<byte[]>();
            Map<Integer, Integer> newRowForColumnIndexMapping = new HashMap<Integer, Integer>();
            int cfIdx = 0, newRowForColumnIdx = 0;
            for (RawTableColumnFamilyDesc cfDesc : cfDescs) {
                int[] colIndex = cfDesc.getColumnIndex(); 
                byte[] currentCfBytes = newRow.get(cfIdx);              
                ByteBuffer buf = ByteBuffer.wrap(currentCfBytes);
                int cfBytesOffset = 0; 
                for (int colIdx : colIndex) {                    
                    int colBytesLength = rawColumnsCodec.getDataTypeSerializer(colIdx).peekLength(buf);
                    buf.position(buf.position() + colBytesLength);
                    byte[] currentColBytes = new byte[colBytesLength];
                    System.arraycopy(currentCfBytes, cfBytesOffset, currentColBytes, 0, colBytesLength);
                    cfBytesOffset += colBytesLength;
                    newRowForColumn.add(currentColBytes);                   
                    newRowForColumnIndexMapping.put(colIdx, newRowForColumnIdx);
                    newRowForColumnIdx++;
                }
                cfIdx++;
            }
            
            // Step 3: transform byte array to byte array list in sortby-first order. 
            List<byte[]> newRowForColumnInOrder = new ArrayList<byte[]>();
            ImmutableBitSet sortbyColumns = mapping.getSortbyColumnSet();
            for (int i : sortbyColumns) {
                newRowForColumnInOrder.add(newRowForColumn.get(newRowForColumnIndexMapping.get(i)));
            }            
            ImmutableBitSet nonSortbyColumns = mapping.getNonSortbyColumnSet();
            for (int i : nonSortbyColumns) {
                newRowForColumnInOrder.add(newRowForColumn.get(newRowForColumnIndexMapping.get(i)));
            }            
            key = new ByteArrayListWritable(newRowForColumnInOrder);

            return true;
        }

        @Override
        public ByteArrayListWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public ByteArrayListWritable getCurrentValue() throws IOException, InterruptedException {
            return val;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}