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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Used to build parquet inverted index
 * @param <K> Dimension values
 * @param <V> Page Id
 */
public class ParquetRawTablePageInputFormat<K, V> extends FileInputFormat<K, V> {
    public org.apache.hadoop.mapreduce.RecordReader<K, V> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetRawTablePageReader<>();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class ParquetRawTablePageReader<K, V> extends RecordReader<K, V> {
        protected static final Logger logger = LoggerFactory.getLogger(ParquetRawTablePageReader.class);

        protected Configuration conf;
        private KylinConfig kylinConfig;
        private RawTableInstance rawTableInstance;
        private RawTableDesc rawTableDesc;
        private BufferedRawColumnCodec rawColumnsCodec;
        private RawTableColumnFamilyDesc[] cfDescs;
        private Map<Integer, Integer> index2OrderMapping;
        private RawToGridTableMapping mapping;
        
        private Path shardPath;
        private ParquetBundleReader reader = null;

        private K key;
        private V val;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            shardPath = fileSplit.getPath();

            logger.info("shard file path: {}", shardPath.toString());
            
            kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
            String rawName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            logger.info("RawTableName is " + rawName);
            rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(rawName);
            rawTableDesc = rawTableInstance.getRawTableDesc();
            GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableDesc);
            rawColumnsCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());
            cfDescs = rawTableDesc.getRawTableMapping().getColumnFamily();
            index2OrderMapping = rawTableDesc.getOrigin2OrderMapping();
            mapping = rawTableDesc.getRawToGridTableMapping();
            // init with first shard file
            reader = new ParquetBundleReader.Builder().setConf(conf).setPath(shardPath).build();
        }

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
                    int columnIndexInOrder = index2OrderMapping.get(colIdx);
                    int colBytesLength = rawColumnsCodec.getDataTypeSerializer(columnIndexInOrder).peekLength(buf);
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
            
            // Step 3: transform byte array to byte array list in original order. 
            List<byte[]> newRowForColumnInOriginalOrder = new ArrayList<byte[]>();
            for (int i = 0; i < mapping.getOriginColumns().size(); i++) {
                newRowForColumnInOriginalOrder.add(newRowForColumn.get(newRowForColumnIndexMapping.get(i)));
            }
            
            key = (K) new ByteArrayListWritable(newRowForColumnInOriginalOrder);
            val = (V) new IntWritable(reader.getPageIndex());
            return true;
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
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

        // Return a list present extended columns index,
        // as dimensions is in the first column, add 1 to result index
        private ImmutableRoaringBitmap countExtendedColumn(CubeInstance cube) {
            List<MeasureDesc> measures = cube.getMeasures();
            int len = measures.size();
            MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
            for (int i = 0; i < len; ++i) {

                // TODO: wrapper to a util function
                if (measures.get(i).getFunction().getExpression().equalsIgnoreCase("EXTENDED_COLUMN")) {
                    bitmap.add(i + 1);
                }
            }

            return bitmap.toImmutableRoaringBitmap();
        }
    }
}