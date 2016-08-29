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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileReader;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;

public class SparkParquetVisit implements Serializable {

    public static final Logger logger = LoggerFactory.getLogger(SparkParquetVisit.class);

    private final transient JavaSparkContext sc;
    private final transient SparkJobProtos.SparkJobRequest request;
    private final transient KylinConfig kylinConfig;
    private final transient String parquetPath;
    private final transient String realizationType;

    public SparkParquetVisit(JavaSparkContext sc, SparkJobProtos.SparkJobRequest request) {
        try {
            this.sc = sc;
            this.request = request;
            this.kylinConfig = KylinConfig.createKylinConfig(request.getKylinProperties());
            this.realizationType = request.getRealizationType();

            if (RealizationType.CUBE.equals(this.realizationType)) {
                this.parquetPath = new StringBuilder(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                        append(request.getRealizationId()).append("/").//
                        append(request.getSegmentId()).append("/").//
                        append(request.getDataFolderName()).//
                        append("/*.parquettar").toString();
            } else {
                this.parquetPath = new StringBuilder(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                        append(request.getRealizationId()).append("/").//
                        append(request.getSegmentId()).append("/").//
                        append(request.getDataFolderName()).//
                        append("/*.parquet").toString();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<byte[]> executeTask() throws Exception {

        Configuration conf = new Configuration();
        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS, RoaringBitmaps.writeToString(request.getParquetColumnsList())); // which columns are required
        conf.set(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH, String.valueOf(request.getMaxRecordLength())); // max gt length
        conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, request.getKylinProperties()); //push down kylin config
        conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES, new String(this.request.getGtScanRequest().toByteArray(), "ISO-8859-1")); //so that ParquetRawInputFormat can use the scan request
        conf.set(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX, String.valueOf(request.getUseII())); //whether to use II
        conf.set(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY, ParquetTarballFileReader.ReadStrategy.COMPACT.toString()); //read fashion

        logger.info("Parquet path is " + parquetPath);
        logger.info("Required Measures: " + StringUtils.join(request.getParquetColumnsList(), ","));
        logger.info("Max GT length: " + request.getMaxRecordLength());
        logger.info("Start to visit cube data with Spark <<<<<<");

        final Accumulator<Long> scannedRecords = sc.accumulator(0L, "Scanned Records", LongAccumulableParam.INSTANCE);
        final Accumulator<Long> collectedRecords = sc.accumulator(0L, "Collected Records", LongAccumulableParam.INSTANCE);

        // visit parquet data file
        Class inputFormatClass = RealizationType.CUBE.equals(this.realizationType) ? ParquetTarballFileInputFormat.class : ParquetRawTableFileInputFormat.class;
        JavaPairRDD<Text, Text> seed = sc.newAPIHadoopFile(parquetPath, inputFormatClass, Text.class, Text.class, conf);

        List<byte[]> collected = seed.mapPartitions(new SparkExecutorPreAggFunction(realizationType, scannedRecords, collectedRecords)).collect();
        logger.info(">>>>>> End of visiting cube data with Spark");
        logger.info("The result blob count is " + collected.size());
        return collected;
    }

}
