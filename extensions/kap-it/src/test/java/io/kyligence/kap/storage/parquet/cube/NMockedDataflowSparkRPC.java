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

package io.kyligence.kap.storage.parquet.cube;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

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
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.DummyPartitionStreamer;
import org.apache.kylin.storage.gtrecord.StorageResponseGTScatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.storage.parquet.NDataflowSparkRPC;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.RDDPartitionResult;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkExecutorPreAggFunction;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;
import scala.Tuple2;

@SuppressWarnings("unused")
public class NMockedDataflowSparkRPC extends NDataflowSparkRPC {

    public static final Logger logger = LoggerFactory.getLogger(NMockedDataflowSparkRPC.class);

    public NMockedDataflowSparkRPC(ISegment segment, NCuboidLayout cuboid, GTInfo info, StorageContext context) {
        super(segment, cuboid, info, context);
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
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);

        String dataFolder = new StringBuilder(kapConfig.getReadParquetStoragePath()).//
                append(dataSegment.getDataflow().getUuid()).append("/").//
                append(dataSegment.getId()).append("/").//
                append(cuboid.getId()).//
                append("/*.parquettar").toString();

        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS,
                RoaringBitmaps.writeToString(getRequiredParquetColumns(scanRequest))); // which columns are required
        conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, kylinConfig.exportToString()); //push down kylin config
        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES, new String(scanRequest.toByteArray(), "ISO-8859-1")); //so that ParquetRawInputFormat can use the scan request
        conf.set(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX, String.valueOf(true)); //whether to use II
        conf.set(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY,
                ParquetTarballFileInputFormat.ParquetTarballFileReader.ReadStrategy.COMPACT.toString()); //read fashion

        Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, dataFolder);

        ParquetTarballFileInputFormat inputFormat = new ParquetTarballFileInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(job);

        try {
            List<Iterable<byte[]>> shardRecords = Lists.newArrayList();
            List<ParquetRecordIterator> parquetRecordIterators = Lists.newArrayList();

            for (int i = 0; i < splits.size(); i++) {
                ParquetRecordIterator iterator = new ParquetRecordIterator(job, inputFormat, splits.get(i));
                SparkExecutorPreAggFunction function = new SparkExecutorPreAggFunction(null, null,
                        dataSegment.getDataflow().getType(), "queryonmockedrpc");
                Iterator<byte[]> ret = Iterators.transform(function.call(iterator),
                        new Function<RDDPartitionResult, byte[]>() {
                            @Nullable
                            @Override
                            public byte[] apply(@Nullable RDDPartitionResult input) {
                                return input.getData();
                            }
                        });
                shardRecords.add(Lists.<byte[]> newArrayList(ret));
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

            return new StorageResponseGTScatter(scanRequest, new DummyPartitionStreamer(mockedShardBlobs.iterator()),
                    context);
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

        public ParquetRecordIterator(Job job, FileInputFormat<Text, Text> inputFormat, InputSplit inputSplit)
                throws IOException, InterruptedException {
            TaskAttemptContext context = MapReduceTestUtil.createDummyMapTaskAttemptContext(job.getConfiguration());
            reader = inputFormat.createRecordReader(inputSplit, context);
            MapContext<Text, Text, Text, Text> mcontext = new MapContextImpl<Text, Text, Text, Text>(
                    job.getConfiguration(), context.getTaskAttemptID(), reader, null, null,
                    MapReduceTestUtil.createDummyReporter(), inputSplit);
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
