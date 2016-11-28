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
    private final String queryId;

    public SparkExecutorPreAggFunction(String queryId, String realizationType, Accumulator<Long> scannedRecords, Accumulator<Long> collectedRecords) {
        this.queryId = queryId;
        this.realizationType = realizationType;
        this.scannedRecords = scannedRecords;
        this.collectedRecords = collectedRecords;
    }

    @Override
    public Iterable<byte[]> call(Iterator<Tuple2<Text, Text>> tuple2Iterator) throws Exception {

        logger.info("Working for query with id {}", queryId);
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

            //if it's doing storage aggr, then should rely on GTAggregateScanner's limit check
            if (!gtScanRequest.isDoingStorageAggregation() && counter >= gtScanRequest.getStoragePushDownLimit()) {
                //read one more record than limit
                logger.info("The finalScanner aborted because storagePushDownLimit is satisfied");
                break;
            }
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
