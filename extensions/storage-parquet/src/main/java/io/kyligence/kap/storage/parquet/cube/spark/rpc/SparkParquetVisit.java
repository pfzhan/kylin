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

package io.kyligence.kap.storage.parquet.cube.spark.rpc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetSpliceTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.filter.BinaryFilter;
import io.kyligence.kap.storage.parquet.format.filter.BinaryFilterConverter;
import io.kyligence.kap.storage.parquet.format.filter.BinaryFilterSerializer;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;
import io.kyligence.kap.storage.parquet.steps.ParquetCubeInfoCollectionStep;
import scala.Tuple4;

public class SparkParquetVisit implements Serializable {
    public static final Logger logger = LoggerFactory.getLogger(SparkParquetVisit.class);

    private final transient JavaSparkContext sc;
    private final transient SparkJobProtos.SparkJobRequestPayload request;
    private final transient KylinConfig kylinConfig;
    private final transient boolean isSplice;
    private final transient Set<String> parquetPathCollection;
    private final transient String realizationType;
    private final transient String dataFolderName;
    private transient byte[] binaryFilterSerialized = null;

    private final static ExecutorService cachedRDDCleaner = Executors.newSingleThreadExecutor();
    private final static ConcurrentLinkedDeque<Tuple4<String, Iterator<RDDPartitionData>, JavaRDD<byte[]>, Long>> cachedRDDs = new ConcurrentLinkedDeque<>();//queryid,iterator,rdd,inqueue time
    private final static Map<String, Map<Long, Set<String>>> cubeMappingCache;
    static {
        //cuboid to file mapping for each cube
        cubeMappingCache = Maps.newConcurrentMap();
        //avoid cached RDD to occupy executor heap for too long
        cachedRDDCleaner.submit(new Runnable() {
            @Override
            public void run() {
                List<Tuple4<String, Iterator<RDDPartitionData>, JavaRDD<byte[]>, Long>> pendingList = new ArrayList<>();
                while (true) {
                    for (Tuple4<String, Iterator<RDDPartitionData>, JavaRDD<byte[]>, Long> t : pendingList) {
                        logger.info("unpersist cached RDD {} from query {}", t._3(), t._1());
                        t._3().unpersist();
                    }
                    pendingList.clear();

                    Iterator<Tuple4<String, Iterator<RDDPartitionData>, JavaRDD<byte[]>, Long>> iterator = cachedRDDs.iterator();
                    while (iterator.hasNext()) {
                        Tuple4<String, Iterator<RDDPartitionData>, JavaRDD<byte[]>, Long> next = iterator.next();
                        if (!next._2().hasNext()) {
                            logger.info("add RDD {} from query {} to unpersist list because its iterator is drained", next._3(), next._1());
                            pendingList.add(next);
                            iterator.remove();
                        }

                        //protect against potential memory leak
                        if (System.currentTimeMillis() - next._4() > 600000) {
                            logger.info("add RDD {} from query {} to unpersist list because it has been inqueued for too long", next._3(), next._1());
                            pendingList.add(next);
                            iterator.remove();
                        }
                    }

                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        logger.error("error sleeping", e);
                    }
                }
            }
        });
    }

    public SparkParquetVisit(JavaSparkContext sc, SparkJobProtos.SparkJobRequestPayload request) throws ClassNotFoundException {
        try {
            this.sc = sc;
            this.request = request;
            this.realizationType = request.getRealizationType();
            this.dataFolderName = request.getDataFolderName();
            KylinConfig.setKylinConfigInEnvIfMissing(request.getKylinProperties());
            this.kylinConfig = KylinConfig.getInstanceFromEnv();

            if (RealizationType.CUBE.toString().equals(this.realizationType)) {
                Map<Long, Set<String>> cubeMapping = readCubeMappingInfo();

                if (cubeMapping == null) {
                    // Engine 100
                    this.isSplice = false;
                    this.parquetPathCollection = Sets.newHashSet();
                    this.parquetPathCollection.add(new StringBuilder(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                            append(request.getRealizationId()).append("/").//
                            append(request.getSegmentId()).append("/").//
                            append(request.getDataFolderName()).//
                            append("/*.parquettar").toString());
                    // try to convert binary filter
                    GTScanRequest scanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(request.getGtScanRequest().toByteArray()));
                    if (scanRequest != null && scanRequest.getFilterPushDown() != null && !BinaryFilterConverter.containsSpecialFilter(scanRequest.getFilterPushDown())) {
                        BinaryFilter binaryFilter = new BinaryFilterConverter(scanRequest.getInfo()).toBinaryFilter(scanRequest.getFilterPushDown());
                        binaryFilterSerialized = BinaryFilterSerializer.serialize(binaryFilter);
                    }
                } else {
                    // Engine 99
                    this.isSplice = true;
                    this.parquetPathCollection = cubeMapping.get(Long.parseLong(request.getDataFolderName()));
                }
            } else {
                this.isSplice = false;
                this.parquetPathCollection = Sets.newHashSet();
                this.parquetPathCollection.add(new StringBuilder(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                        append(request.getRealizationId()).append("/").//
                        append(request.getSegmentId()).append("/").//
                        append(request.getDataFolderName()).//
                        append("/*.parquet.inv").toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Long, Set<String>> readCubeMappingInfo() throws IOException, ClassNotFoundException {
        String cubeInfoPath = new StringBuilder(kylinConfig.getHdfsWorkingDirectory()).append("parquet/")//
                .append(request.getRealizationId()).append("/")//
                .append(request.getSegmentId()).append("/")//
                .append(ParquetCubeInfoCollectionStep.CUBE_INFO_NAME).toString();
        if (cubeMappingCache.containsKey(cubeInfoPath)) {
            return cubeMappingCache.get(cubeInfoPath);
        }

        FileSystem fs = HadoopUtil.getFileSystem(cubeInfoPath);
        if (fs.exists(new Path(cubeInfoPath))) {
            Map<Long, Set<String>> map;
            try (ObjectInputStream inputStream = new ObjectInputStream(fs.open(new Path(cubeInfoPath)))) {
                map = (Map<Long, Set<String>>) inputStream.readObject();
                cubeMappingCache.put(cubeInfoPath, map);
            }
            return map;
        } else {
            return null;
        }
    }

    Iterator<RDDPartitionData> executeTask() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS, RoaringBitmaps.writeToString(request.getParquetColumnsList())); // which columns are required
        conf.set(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH, String.valueOf(request.getMaxRecordLength())); // max gt length
        conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, request.getKylinProperties()); //push down kylin config
        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES, new String(this.request.getGtScanRequest().toByteArray(), "ISO-8859-1")); //so that ParquetRawInputFormat can use the scan request
        conf.set(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX, String.valueOf(request.getUseII())); //whether to use II
        conf.set(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY, ParquetTarballFileInputFormat.ParquetTarballFileReader.ReadStrategy.COMPACT.toString()); //read fashion
        conf.set(ParquetFormatConstants.KYLIN_BINARY_FILTER, new String(binaryFilterSerialized == null ? new byte[0] : binaryFilterSerialized, "ISO-8859-1")); //read fashion

        StringBuilder pathBuilder = new StringBuilder();
        for (String p : parquetPathCollection) {
            pathBuilder.append(p).append(";");
        }
        logger.info("Columnar path is " + pathBuilder.toString());
        logger.info("Required Measures: " + StringUtils.join(request.getParquetColumnsList(), ","));
        logger.info("Max GT length: " + request.getMaxRecordLength());
        logger.info("Current queryId: " + request.getQueryId());
        logger.info("Start to visit cube data with Spark <<<<<<");

        final Accumulator<Long> scannedRecords = sc.accumulator(0L, "Scanned Records", LongAccumulableParam.INSTANCE);
        final Accumulator<Long> collectedRecords = sc.accumulator(0L, "Collected Records", LongAccumulableParam.INSTANCE);

        // visit parquet data file
        Class inputFormatClass = RealizationType.CUBE.toString().equals(this.realizationType) ? ParquetTarballFileInputFormat.class : ParquetRawTableFileInputFormat.class;
        if (isSplice && RealizationType.CUBE.toString().equals(this.realizationType)) {
            inputFormatClass = ParquetSpliceTarballFileInputFormat.class;
            conf.set(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS, this.dataFolderName);
        }

        JavaPairRDD[] rdds = new JavaPairRDD[parquetPathCollection.size()];
        int index = 0;
        for (String p : parquetPathCollection) {
            rdds[index++] = sc.newAPIHadoopFile(p, inputFormatClass, Text.class, Text.class, conf);
        }
        JavaPairRDD<Text, Text> seed = sc.union(rdds);

        final Iterator<RDDPartitionData> partitionResults;
        JavaRDD<byte[]> cached = seed.mapPartitions(new SparkExecutorPreAggFunction(request.getQueryId(), realizationType, scannedRecords, collectedRecords, isSplice, hasPreFiltered(), request.getSpillEnabled(), request.getMaxScanBytes())).cache();
        cached.count();//trigger lazy materialization
        long scanCount = collectedRecords.value();
        long threshold = (long) kylinConfig.getLargeQueryThreshold();
        logger.info("The threshold for large result set is {}, current count is {}", threshold, scanCount);
        if (scanCount > threshold) {
            logger.info("returning large result set");
            partitionResults = wrap(cached.toLocalIterator());
            cachedRDDs.add(new Tuple4<>(request.getQueryId(), partitionResults, cached, System.currentTimeMillis())); //will be cleaned later
        } else {
            logger.info("returning normal result set");
            partitionResults = wrap(cached.collect().iterator());
            cached.unpersist();
        }

        logger.info(">>>>>> End of visiting cube data with Spark");
        logger.info("The result blob count is {}, the scanned count is {} and the collected count is {}", scanCount, scannedRecords.value(), collectedRecords.value());
        return partitionResults;
    }

    private Iterator<RDDPartitionData> wrap(Iterator<byte[]> bytesIter) {
        return Iterators.transform(bytesIter, new Function<byte[], RDDPartitionData>() {
            @Nullable
            @Override
            public RDDPartitionData apply(@Nullable byte[] bytes) {
                return new RDDPartitionData(bytes);
            }
        });
    }

    private boolean hasPreFiltered() {
        return binaryFilterSerialized != null;
    }

    public class RDDPartitionData {
        private final byte[] data;

        public RDDPartitionData(byte[] data) {
            this.data = data;
        }

        public byte[] getData() {
            return data;
        }
    }
}
