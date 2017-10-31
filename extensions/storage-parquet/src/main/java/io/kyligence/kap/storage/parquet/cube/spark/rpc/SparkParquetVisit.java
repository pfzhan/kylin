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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.kylin.shaded.htrace.org.apache.htrace.TraceInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.filter.TupleFilterSerializerRawTableExt;
import io.kyligence.kap.metadata.model.IKapStorageAware;
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

@SuppressWarnings("Duplicates")
public class SparkParquetVisit implements Serializable {
    public static final Logger logger = LoggerFactory.getLogger(SparkParquetVisit.class);

    private final transient JavaSparkContext sc;
    private final transient SparkJobProtos.SparkJobRequestPayload request;
    private final transient String streamIdentifier;
    private final transient KylinConfig kylinConfig;
    private final transient KapConfig kapConfig;
    private final transient boolean isSplice;
    private final transient Set<String> parquetPathCollection;
    private final transient Iterator<String> parquetPathIter;
    private final transient String realizationType;
    private final transient int storageType;
    private final transient String dataFolderName;
    private transient byte[] binaryFilterSerialized = null;
    private final transient Class inputFormatClass;
    private final transient Configuration conf;
    private final transient int parallel;
    private transient boolean needLazy;
    private transient int limit = 0;
    private transient long accumulateCnt = 0;
    private transient String workingDir = null;
    private transient JavaPairRDD batchRdd = null;
    private static final transient String SCHEMA_HINT = "://";
    private final static Cache<String, Map<Long, Set<String>>> cubeMappingCache = CacheBuilder.newBuilder()
            .maximumSize(10000).expireAfterWrite(1, TimeUnit.HOURS).removalListener(//
                    new RemovalListener<String, Map<Long, Set<String>>>() {
                        @Override
                        public void onRemoval(RemovalNotification<String, Map<Long, Set<String>>> notification) {
                            SparkParquetVisit.logger.info("cubeMappingCache entry with key {} is removed due to {} ",
                                    notification.getKey(), notification.getCause());
                        }
                    })
            .build();
    private final static Cache<String, Set<String>> cubePathCache = CacheBuilder.newBuilder().maximumSize(10000)
            .expireAfterWrite(1, TimeUnit.HOURS).removalListener(//
                    new RemovalListener<String, Set<String>>() {
                        @Override
                        public void onRemoval(RemovalNotification<String, Set<String>> notification) {
                            SparkParquetVisit.logger.info("cubePathCache entry with key {} is removed due to {} ",
                                    notification.getKey(), notification.getCause());
                        }
                    })
            .build();

    public SparkParquetVisit(JavaSparkContext sc, SparkJobProtos.SparkJobRequestPayload request,
            String streamIdentifier) {
        try {
            this.streamIdentifier = streamIdentifier;
            this.sc = sc;
            this.request = request;
            this.realizationType = request.getRealizationType();
            this.storageType = request.getStorageType();
            this.dataFolderName = request.getDataFolderName();
            KylinConfig.setKylinConfigInEnvIfMissing(request.getKylinProperties());
            this.kylinConfig = KylinConfig.getInstanceFromEnv();
            this.conf = new Configuration();
            this.kapConfig = KapConfig.wrap(kylinConfig);
            TupleFilterSerializerRawTableExt.getExtendedTupleFilters();//touch static initialization
            GTScanRequest scanRequest = GTScanRequest.serializer
                    .deserialize(ByteBuffer.wrap(request.getGtScanRequest().toByteArray()));
            this.needLazy = false;
            if (kapConfig.getParquetSparkDynamicResourceEnabled()) {
                this.parallel = kapConfig.getParquetSparkExecutorCore()
                        * kapConfig.getParquetSparkExecutorInstanceMax();
            } else {
                this.parallel = kapConfig.getParquetSparkExecutorCore() * kapConfig.getParquetSparkExecutorInstance();
            }

            long startTime = System.currentTimeMillis();
            if (CubeInstance.REALIZATION_TYPE.equals(this.realizationType)) {
                sc.setLocalProperty("spark.scheduler.pool", "cube");

                if (IKapStorageAware.ID_SPLICE_PARQUET != this.storageType) {
                    // Engine 100
                    this.isSplice = false;
                    long listFileStartTime = System.currentTimeMillis();
                    this.parquetPathCollection = listFilesWithCache(
                            new StringBuilder(kapConfig.getReadParquetStoragePath()).//
                                    append(request.getRealizationId()).append("/").//
                                    append(request.getSegmentId()).append("/").//
                                    append(request.getDataFolderName()).toString(),
                            "parquettar");
                    logger.info("listFile takes {} ms", System.currentTimeMillis() - listFileStartTime);
                    this.inputFormatClass = ParquetTarballFileInputFormat.class;
                } else {
                    // Engine 99
                    this.isSplice = true;
                    long readCubeMappingStartTime = System.currentTimeMillis();
                    Map<Long, Set<String>> cubeMapping = readCubeMappingInfo();
                    this.parquetPathCollection = cubeMapping.get(Long.parseLong(request.getDataFolderName()));
                    logger.info("readCubeMapping takes {} ms", System.currentTimeMillis() - readCubeMappingStartTime);
                    this.inputFormatClass = ParquetSpliceTarballFileInputFormat.class;
                    conf.set(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS, this.dataFolderName);
                }

                // try to convert binary filter
                if (scanRequest != null && scanRequest.getFilterPushDown() != null
                        && !BinaryFilterConverter.containsSpecialFilter(scanRequest.getFilterPushDown())) {
                    BinaryFilter binaryFilter = new BinaryFilterConverter(scanRequest.getInfo())
                            .toBinaryFilter(scanRequest.getFilterPushDown());
                    binaryFilterSerialized = BinaryFilterSerializer.serialize(binaryFilter);
                }
            } else {
                sc.setLocalProperty("spark.scheduler.pool", "table_index");
                this.isSplice = false;
                this.parquetPathCollection = listFilesWithCache(
                        new StringBuilder(kapConfig.getReadParquetStoragePath()).//
                                append(request.getRealizationId()).append("/").//
                                append(request.getSegmentId()).append("/").//
                                append(request.getDataFolderName()).toString(),
                        "parquet.inv");
                this.inputFormatClass = ParquetRawTableFileInputFormat.class;
                this.needLazy = StorageContext.isValidPushDownLimit(scanRequest.getStoragePushDownLimit());
                this.limit = scanRequest.getStoragePushDownLimit();
            }

            this.parquetPathIter = Iterators.transform(parquetPathCollection.iterator(), new PathTransformer());
            conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS,
                    RoaringBitmaps.writeToString(request.getParquetColumnsList())); // which columns are required
            conf.set(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH, String.valueOf(request.getMaxRecordLength())); // max gt length
            conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, request.getKylinProperties()); //push down kylin config
            conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES,
                    new String(this.request.getGtScanRequest().toByteArray(), "ISO-8859-1")); //so that ParquetRawInputFormat can use the scan request
            conf.set(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX, String.valueOf(request.getUseII())); //whether to use II
            conf.set(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY,
                    ParquetTarballFileInputFormat.ParquetTarballFileReader.ReadStrategy.COMPACT.toString()); //read fashion
            conf.set(ParquetFormatConstants.KYLIN_BINARY_FILTER,
                    new String(binaryFilterSerialized == null ? new byte[0] : binaryFilterSerialized, "ISO-8859-1")); //read fashion

            logger.info("SparkVisit Init takes {} ms", System.currentTimeMillis() - startTime);
            StringBuilder pathBuilder = new StringBuilder();
            for (String p : parquetPathCollection) {
                pathBuilder.append(p).append(";");
            }

            logger.info("Columnar path is " + pathBuilder.toString());
            logger.info("Required Measures: " + StringUtils.join(request.getParquetColumnsList(), ","));
            logger.info("Max GT length: " + request.getMaxRecordLength());
            logger.info("needLazy: " + needLazy);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    // Transform relative path to absolute path
    private class PathTransformer implements Function<String, String> {
        private String workingDir = kapConfig.getReadHdfsWorkingDirectory();

        @Nullable
        @Override
        public String apply(@Nullable String input) {
            if (input.contains(SCHEMA_HINT)) {
                return input;
            }
            return workingDir + "/" + input;
        }
    }

    /**
     * warn!!! The method  cache the result. If you use it ,pls ensure this dir never modify.
     * @param directory   FileDir
     * @param suffix      FileSuffix
     * @return File Set
     */
    private Set<String> listFilesWithCache(String directory, final String suffix) throws IOException {
        String key = directory + ":" + suffix;
        Set<String> ifPresent = cubePathCache.getIfPresent(key);
        if (ifPresent != null) {
            return ifPresent;
        }
        Set<String> fileSets = listFiles(directory, suffix);
        cubePathCache.put(key, fileSets);
        return fileSets;
    }

    private Set<String> listFiles(String directory, final String suffix) throws IOException {
        FileSystem fs = HadoopUtil.getFileSystem(directory);
        final FileStatus[] fileStatuses = fs.listStatus(new Path(directory));
        Set<String> result = Sets.newHashSet();
        for (FileStatus fileStatuse : fileStatuses) {
            String path = fileStatuse.getPath().toString();
            if (path.endsWith(suffix)) {
                result.add(path);
            }
        }
        return result;
    }

    private Map<Long, Set<String>> readCubeMappingInfo() throws IOException, ClassNotFoundException {
        String cubeInfoPath = new StringBuilder(kapConfig.getReadParquetStoragePath())//
                .append(request.getRealizationId()).append("/")//
                .append(request.getSegmentId()).append("/")//
                .append(ParquetCubeInfoCollectionStep.CUBE_INFO_NAME).toString();

        Map<Long, Set<String>> ifPresent = cubeMappingCache.getIfPresent(cubeInfoPath);
        if (ifPresent != null) {
            return ifPresent;
        }

        logger.trace("not hit cube mapping");
        FileSystem fs = HadoopUtil.getFileSystem(cubeInfoPath);
        Path cubeInfoPath2 = new Path(cubeInfoPath);
        if (fs.exists(cubeInfoPath2) || lazyCheckExists(fs, cubeInfoPath2)) {
            Map<Long, Set<String>> map;
            try (ObjectInputStream inputStream = new ObjectInputStream(fs.open(new Path(cubeInfoPath)))) {
                map = (Map<Long, Set<String>>) inputStream.readObject();
                cubeMappingCache.put(cubeInfoPath, map);
            }
            return map;
        } else {
            throw new RuntimeException("Cannot find CubeMappingInfo at " + cubeInfoPath);
        }
    }

    /**
     * Check whether the file exists after a list operation. This is for Alluxio's case, which won't find new file until do a list.
     * @param fs
     * @param path
     * @return
     */
    private boolean lazyCheckExists(FileSystem fs, Path path) {
        logger.debug("check whether path exists {}", path.toString());
        if (path.getParent() != null && path.getParent().toString().equals(path.toString()) == false) {
            lazyCheckExists(fs, path.getParent());
        }
        boolean exist = false;
        try {
            fs.listStatus(path);
            exist = fs.exists(path);
        } catch (Exception e) {
            logger.error("error to list parent path", e);
            exist = false;
        }

        return exist;
    }

    Pair<Iterator<RDDPartitionResult>, JavaRDD<RDDPartitionResult>> executeTask() throws Exception {

        logger.info("Start to visit cube data with Spark <<<<<<");
        final Accumulator<Long> scannedRecords = sc.accumulator(0L, "Scanned Records", LongAccumulableParam.INSTANCE);
        final Accumulator<Long> collectedRecords = sc.accumulator(0L, "Collected Records",
                LongAccumulableParam.INSTANCE);

        JavaPairRDD<Text, Text> seed = batchRdd;
        batchRdd = null;

        Trace.addTimelineAnnotation("creating result rdd for one segment");
        final Iterator<RDDPartitionResult> partitionResults;
        JavaRDD<RDDPartitionResult> baseRDD = seed
                .mapPartitions(
                        new SparkExecutorPreAggFunction(scannedRecords, collectedRecords, realizationType, isSplice,
                                hasPreFiltered(), //
                                streamIdentifier, request.getSpillEnabled(), request.getMaxScanBytes(),
                                request.getStartTime(), Trace.isTracing() ? KryoTraceInfo.fromTraceInfo(TraceInfo.fromSpan(Trace.currentSpan())) : null, //
                                kapConfig.diagnosisMetricWriterType())) //
                .cache();

        baseRDD.count();//trigger lazy materialization
        Trace.addTimelineAnnotation("result rdd materialized");
        long scanCount = collectedRecords.value();
        long threshold = (long) kylinConfig.getLargeQueryThreshold();
        logger.info("The threshold for large result set is {}, current count is {}", threshold, scanCount);
        accumulateCnt += scanCount;
        if (scanCount > threshold) {
            logger.info("returning large result set");
            int newPartitions = (int) Math.round((double) scanCount / (threshold * 0.8));
            int oldPartitions = baseRDD.getNumPartitions();
            int partitionRatio = oldPartitions / newPartitions;
            if (oldPartitions > kapConfig.getAutoRepartionThreshold()
                    && partitionRatio > kapConfig.getAutoRepartitionRatio()) {
                logger.info("repartition {} to {}", oldPartitions, newPartitions);
                partitionResults = baseRDD.repartition(newPartitions).toLocalIterator();
            } else {
                partitionResults = baseRDD.toLocalIterator();
            }
        } else {
            logger.info("returning normal result set");
            partitionResults = baseRDD.collect().iterator();
        }

        logger.info("==========================================================");
        logger.info("end of visiting cube data with Spark");
        logger.info("the scanned row count is {} and the collected row count is {}", scanCount, scannedRecords.value(),
                collectedRecords.value());
        logger.info("==========================================================");
        return Pair.newPair(partitionResults, baseRDD);
    }

    public boolean moreRDDExists() {
        // cut stream when limit is met
        if (needLazy && accumulateCnt >= limit) {
            return false;
        }

        if (batchRdd != null) {
            return true;
        }

        StringBuilder sb = new StringBuilder();
        if (needLazy) {
            for (int i = 0; i < parallel; i++) {
                if (parquetPathIter.hasNext()) {
                    sb.append(parquetPathIter.next()).append(",");
                } else {
                    break;
                }
            }
        } else {
            while (parquetPathIter.hasNext()) {
                sb.append(parquetPathIter.next()).append(",");
            }
        }

        String path = sb.toString();
        if (path.isEmpty()) {
            return false;
        }

        batchRdd = sc.newAPIHadoopFile(path.substring(0, path.length() - 1), inputFormatClass, Text.class, Text.class,
                conf);
        return true;
    }

    private boolean hasPreFiltered() {
        return binaryFilterSerialized != null;
    }

}
