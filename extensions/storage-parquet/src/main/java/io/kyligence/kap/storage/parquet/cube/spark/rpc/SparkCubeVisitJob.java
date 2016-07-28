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

package io.kyligence.kap.storage.parquet.cube.spark.rpc;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.OriginalBytesGTScanner;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileReader;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;
import scala.Tuple2;

public class SparkCubeVisitJob implements Serializable {

    public static final Logger logger = LoggerFactory.getLogger(SparkCubeVisitJob.class);

    private transient JavaSparkContext sc;
    private transient SparkJobProtos.SparkJobRequest request;
    private transient KylinConfig kylinConfig;
    private transient String parquetPath;

    public SparkCubeVisitJob(JavaSparkContext sc, SparkJobProtos.SparkJobRequest request) {
        this.sc = sc;
        this.request = request;
        this.kylinConfig = KylinConfig.createKylinConfigFromInputStream(IOUtils.toInputStream(request.getKylinProperties()));
        this.parquetPath = new StringBuilder(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                append(request.getCubeId()).append("/").//
                append(request.getSegmentId()).append("/").//
                append(request.getCuboidId()).//
                append("/*.parquettar").toString();
    }

    public List<byte[]> executeTask() throws Exception {
        Configuration conf = new Configuration();

        // which measure are needed
        conf.set(ParquetFormatConstants.KYLIN_FILTER_MEASURES_BITSET_MAP, RoaringBitmaps.writeToString(request.getRequiredMeasuresList()));

        // max gt length
        conf.set(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH, String.valueOf(request.getMaxRecordLength()));

        //push down kylin config
        conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, request.getKylinProperties());

        //so that ParquetRawInputFormat can use the scan request
        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES, new String(this.request.getGtScanRequest().toByteArray(), "ISO-8859-1"));

        //whether to use II
        conf.set(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX, String.valueOf(request.getUseII()));

        //read fashion
        conf.set(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY, ParquetTarballFileReader.ReadStrategy.COMPACT.toString());

        logger.info("Parquet path is " + parquetPath);
        logger.info("Required Measures: " + StringUtils.join(request.getRequiredMeasuresList(), ","));
        logger.info("Max GT length: " + request.getMaxRecordLength());
        logger.info("Start to visit cube data with Spark <<<<<<");

        final Accumulator<Long> scannedRecords = sc.accumulator(0L, "Scanned Records", LongAccumulableParam.INSTANCE);
        final Accumulator<Long> collectedRecords = sc.accumulator(0L, "Collected Records", LongAccumulableParam.INSTANCE);

        // visit parquet data file
        JavaPairRDD<Text, Text> seed = sc.newAPIHadoopFile(parquetPath, ParquetTarballFileInputFormat.class, Text.class, Text.class, conf);

        List<byte[]> collected = seed.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Text, Text>>, byte[]>() {
            @Override
            public Iterable<byte[]> call(Iterator<Tuple2<Text, Text>> tuple2Iterator) throws Exception {
                Iterator<ByteBuffer> iterator = Iterators.transform(tuple2Iterator, new Function<Tuple2<Text, Text>, ByteBuffer>() {
                    @Nullable
                    @Override
                    public ByteBuffer apply(@Nullable Tuple2<Text, Text> input) {
                        return ByteBuffer.wrap(input._2.getBytes(), 0, input._2.getLength());
                    }
                });

                GTScanRequest gtScanRequest = ParquetTarballFileReader.gtScanRequestThreadLocal.get();

                IGTScanner scanner = new OriginalBytesGTScanner(gtScanRequest.getInfo(), iterator, gtScanRequest);//in
                IGTScanner preAggred = gtScanRequest.decorateScanner(scanner);//process
                CoalesceGTRecordExport function = new CoalesceGTRecordExport(gtScanRequest, gtScanRequest.getColumns());

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Iterator<GTRecord> gtIterator = preAggred.iterator();
                long counter = 0;
                while (gtIterator.hasNext()) {
                    counter++;
                    GTRecord row = gtIterator.next();
                    ByteArray byteArray = function.apply(row);
                    baos.write(byteArray.array(), 0, byteArray.length());
                }
                logger.info("Current task scanned {} raw records", preAggred.getScannedRowCount());
                logger.info("Current task contributing {} results", counter);
                scannedRecords.add(preAggred.getScannedRowCount());
                collectedRecords.add(counter);

                byte[] ret = baos.toByteArray();
                return Collections.singleton(ret);
            }
        }).collect();
        logger.info(">>>>>> End of visiting cube data with Spark");
        logger.info("The result blob count is " + collected.size());
        return collected;
    }
}
