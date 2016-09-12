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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanTimeoutException;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner4Cube;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner4Raw;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableFileReader;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileReader;
import scala.Tuple2;

public class SparkExecutorPreAggFunction implements FlatMapFunction<Iterator<Tuple2<Text, Text>>, byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(SparkExecutorPreAggFunction.class);

    private final Accumulator<Long> scannedRecords;
    private final Accumulator<Long> collectedRecords;
    private final String realizationType;

    public SparkExecutorPreAggFunction(String realizationType, Accumulator<Long> scannedRecords, Accumulator<Long> collectedRecords) {
        this.realizationType = realizationType;
        this.scannedRecords = scannedRecords;
        this.collectedRecords = collectedRecords;
    }

    @Override
    public Iterable<byte[]> call(Iterator<Tuple2<Text, Text>> tuple2Iterator) throws Exception {

        long localStartTime = System.currentTimeMillis();

        Iterator<ByteBuffer> iterator = Iterators.transform(tuple2Iterator, new Function<Tuple2<Text, Text>, ByteBuffer>() {
            @Nullable
            @Override
            public ByteBuffer apply(@Nullable Tuple2<Text, Text> input) {
                return ByteBuffer.wrap(input._2.getBytes(), 0, input._2.getLength());
            }
        });

        GTScanRequest gtScanRequest = null;
        StorageSideBehavior behavior = null;

        IGTScanner scanner;
        if (RealizationType.CUBE.toString().equals(realizationType)) {
            gtScanRequest = ParquetTarballFileReader.gtScanRequestThreadLocal.get();
            behavior = StorageSideBehavior.valueOf(gtScanRequest.getStorageBehavior());
            scanner = new ParquetBytesGTScanner4Cube(gtScanRequest.getInfo(), iterator, gtScanRequest, behavior.delayToggledOn());//in
        } else if (RealizationType.INVERTED_INDEX.toString().equals(realizationType)) {
            gtScanRequest = ParquetRawTableFileReader.gtScanRequestThreadLocal.get();
            behavior = StorageSideBehavior.valueOf(gtScanRequest.getStorageBehavior());
            scanner = new ParquetBytesGTScanner4Raw(gtScanRequest.getInfo(), iterator, gtScanRequest, behavior.delayToggledOn());//in
        } else {
            throw new IllegalArgumentException("Unsupported realization type " + realizationType);
        }

        long deadline = gtScanRequest.getTimeout() + localStartTime;
        logger.info("Local start time is {} and the deadline is {}", localStartTime, deadline);

        IGTScanner preAggred = gtScanRequest.decorateScanner(scanner, behavior.filterToggledOn(), behavior.aggrToggledOn(), deadline);

        SparkExecutorGTRecordSerializer function = new SparkExecutorGTRecordSerializer(gtScanRequest, gtScanRequest.getColumns());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Iterator<GTRecord> gtIterator = preAggred.iterator();
        long counter = 0;
        while (gtIterator.hasNext()) {

            //check deadline
            if (counter % GTScanRequest.terminateCheckInterval == 1 && System.currentTimeMillis() > deadline) {
                throw new GTScanTimeoutException("Timeout in GTAggregateScanner with scanned count " + counter);
            }

            GTRecord row = gtIterator.next();
            ByteArray byteArray = function.apply(row);
            baos.write(byteArray.array(), 0, byteArray.length());

            counter++;
        }

        logger.info("Current task scanned {} raw records", preAggred.getScannedRowCount());
        logger.info("Current task contributing {} results", counter);

        if (scannedRecords != null)
            scannedRecords.add(preAggred.getScannedRowCount());
        if (collectedRecords != null)
            collectedRecords.add(counter);

        byte[] ret = baos.toByteArray();
        return Collections.singleton(ret);
    }
}
