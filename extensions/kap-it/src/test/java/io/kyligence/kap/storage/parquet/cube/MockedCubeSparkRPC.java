/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.storage.parquet.cube;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.gtrecord.StorageResponseGTScatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkExecutorPreAggFunction;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;
import scala.Tuple2;

@SuppressWarnings("unused")
public class MockedCubeSparkRPC extends CubeSparkRPC {

    public static final Logger logger = LoggerFactory.getLogger(MockedCubeSparkRPC.class);

    public MockedCubeSparkRPC(ISegment segment, Cuboid cuboid, GTInfo info) {
        super(segment, cuboid, info);
    }

    @Override
    protected void init() {
        //do nothing
    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException {

        scanRequest.setTimeout(KapConfig.getInstanceFromEnv().getSparkVisitTimeout());
        logger.info("Spark visit timeout is set to " + scanRequest.getTimeout());

        Configuration conf = HadoopUtil.getCurrentConfiguration();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        String dataFolder = new StringBuilder(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                append(cubeSegment.getCubeInstance().getUuid()).append("/").//
                append(cubeSegment.getUuid()).append("/").//
                append(cuboid.getId()).//
                append("/*.parquettar").toString();

        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS, RoaringBitmaps.writeToString(getRequiredParquetColumns(scanRequest))); // which columns are required
        conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, kylinConfig.getConfigAsString()); //push down kylin config
        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES, new String(scanRequest.toByteArray(), "ISO-8859-1")); //so that ParquetRawInputFormat can use the scan request
        conf.set(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX, String.valueOf(true)); //whether to use II
        conf.set(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY, ParquetTarballFileInputFormat.ParquetTarballFileReader.ReadStrategy.COMPACT.toString()); //read fashion

        Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, dataFolder);

        ParquetTarballFileInputFormat inputFormat = new ParquetTarballFileInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(job);

        try {
            List<Iterable<byte[]>> shardRecords = Lists.newArrayList();
            List<ParquetRecordIterator> parquetRecordIterators = Lists.newArrayList();

            for (int i = 0; i < splits.size(); i++) {
                ParquetRecordIterator iterator = new ParquetRecordIterator(job, inputFormat, splits.get(i));
                SparkExecutorPreAggFunction function = new SparkExecutorPreAggFunction("queryonmockedrpc", RealizationType.CUBE.toString(), null, null);
                Iterable<byte[]> ret = function.call(iterator);
                shardRecords.add(ret);
                parquetRecordIterators.add(iterator);
            }

            List<byte[]> mockedShardBlobs = Lists.newArrayList();
            for (Iterable<byte[]> shard : shardRecords) {
                mockedShardBlobs.add(concat(shard));
            }

            //remember to close
            for (Closeable closeable : parquetRecordIterators) {
                closeable.close();
            }

            return new StorageResponseGTScatter(info, mockedShardBlobs.iterator(), scanRequest.getColumns(), 0, scanRequest.getStoragePushDownLimit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] concat(Iterable<byte[]> rows) {
        int length = 0;
        for (byte[] row : rows) {
            length += row.length;
        }

        byte[] ret = new byte[length];
        int offset = 0;
        for (byte[] row : rows) {
            System.arraycopy(row, 0, ret, offset, row.length);
            offset += row.length;
        }
        return ret;
    }

    private class ParquetRecordIterator implements Iterator<Tuple2<Text, Text>>, Closeable {
        private boolean fetched = false;
        private Tuple2<Text, Text> buffer = new Tuple2<>(null, null);
        private RecordReader<Text, Text> reader;

        public ParquetRecordIterator(Job job, FileInputFormat<Text, Text> inputFormat, InputSplit inputSplit) throws IOException, InterruptedException {
            TaskAttemptContext context = MapReduceTestUtil.createDummyMapTaskAttemptContext(job.getConfiguration());
            reader = inputFormat.createRecordReader(inputSplit, context);
            MapContext<Text, Text, Text, Text> mcontext = new MapContextImpl<Text, Text, Text, Text>(job.getConfiguration(), context.getTaskAttemptID(), reader, null, null, MapReduceTestUtil.createDummyReporter(), inputSplit);
            reader.initialize(inputSplit, mcontext);
        }

        @Override
        public boolean hasNext() {
            if (fetched) {
                return true;
            }
            try {
                if (reader.nextKeyValue()) {
                    Text a = reader.getCurrentKey();
                    Text b = reader.getCurrentValue();
                    buffer = new Tuple2<>(a, b);
                    fetched = true;
                    return true;
                } else {
                    return false;
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Tuple2<Text, Text> next() {
            if (!fetched) {
                if (!hasNext()) {
                    throw new IllegalStateException("No more");
                }
            }
            fetched = false;
            return buffer;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
