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
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
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

    //    private Broadcast<byte[]> bcGtReq;
    //    private Broadcast<String> bcKylinProperties;
    private Broadcast<CanonicalCuboid> bcCanonicalCuboid;

    public SparkCubeVisitJob(JavaSparkContext sc, SparkJobProtos.SparkJobRequest request) {
        this.sc = sc;
        this.request = request;
        this.kylinConfig = KylinConfig.createKylinConfigFromInputStream(IOUtils.toInputStream(request.getKylinProperties()));

        //this.bcGtReq = sc.broadcast(request.getGtScanRequest().toByteArray());
        //this.bcKylinProperties = sc.broadcast(request.getKylinProperties());
        this.bcCanonicalCuboid = sc.broadcast(new CanonicalCuboid(request.getCubeId(), request.getSegmentId(), request.getCuboidId()));
    }

    public List<byte[]> executeTask() {
        String basePath = new StringBuffer(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                append(bcCanonicalCuboid.getValue().getCubeId()).append("/").//
                append(bcCanonicalCuboid.getValue().getSegmentId()).append("/").//
                append(bcCanonicalCuboid.getValue().getCuboidId()).toString();

        String parquetPath = basePath + "/*.parquettar";

        Configuration conf = new Configuration();

        logger.info("Required Measures: " + StringUtils.join(request.getRequiredMeasuresList(), ","));
        conf.set(ParquetFormatConstants.KYLIN_FILTER_MEASURES_BITSET_MAP, RoaringBitmaps.writeToString(request.getRequiredMeasuresList()));

        logger.info("Max GT length: " + request.getMaxRecordLength());
        conf.set(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH, String.valueOf(request.getMaxRecordLength()));

        conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, request.getKylinProperties());
        try {
            //so that ParquetRawInputFormat can use the scan request
            conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES, new String(this.request.getGtScanRequest().toByteArray(), "ISO-8859-1"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        logger.info("================Cube Data Start==================");
        // visit parquet data file
        logger.info("Parquet path is " + parquetPath);
        JavaPairRDD<byte[], byte[]> seed = sc.newAPIHadoopFile(parquetPath, ParquetTarballFileInputFormat.class, byte[].class, byte[].class, conf);
        List<byte[]> collected = seed.mapPartitions(new FlatMapFunction<Iterator<Tuple2<byte[], byte[]>>, byte[]>() {
            @Override
            public Iterable<byte[]> call(Iterator<Tuple2<byte[], byte[]>> tuple2Iterator) throws Exception {
                Iterator<byte[]> iterator = Iterators.transform(tuple2Iterator, new Function<Tuple2<byte[], byte[]>, byte[]>() {
                    @Nullable
                    @Override
                    public byte[] apply(@Nullable Tuple2<byte[], byte[]> input) {
                        return input._2;
                    }
                });

                GTScanRequest gtScanRequest = ParquetTarballFileReader.gtScanRequestThreadLocal.get();

                IGTScanner scanner = new OriginalBytesGTScanner(gtScanRequest.getInfo(), iterator, gtScanRequest);//in
                IGTScanner preAggred = gtScanRequest.decorateScanner(scanner);//process
                return Iterables.transform(preAggred, new CoalesceGTRecordExport(gtScanRequest, gtScanRequest.getColumns()));//out
            }
        }).collect();
        logger.info("================Cube Data End==================");

        logger.info("The result size is " + collected.size());
        return collected;
    }
}
