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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.kylin.common.htrace.HtraceInit;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.StorageLimitLevel;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.kyligence.kap.common.obf.IKeepClassMembers;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner4Cube;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner4Raw;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetSpliceTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import scala.Tuple2;

public class SparkExecutorPreAggFunction
        implements IKeepClassMembers, FlatMapFunction<Iterator<Tuple2<Text, Text>>, RDDPartitionResult> {
    private static final Logger logger = LoggerFactory.getLogger(SparkExecutorPreAggFunction.class);

    private final Accumulator<Long> scannedRecords;
    private final Accumulator<Long> collectedRecords;
    private final String realizationType;
    private final String streamIdentifier;
    private final long maxScannedBytes;
    private final boolean isSplice;
    private final boolean hasPreFiltered;
    private final boolean spillEnabled;
    private final long startTime;

    private final KryoTraceInfo kryoTraceInfo;

    public SparkExecutorPreAggFunction(Accumulator<Long> scannedRecords, Accumulator<Long> collectedRecords,
            String realizationType, String streamIdentifier) {
        this(scannedRecords, collectedRecords, realizationType, false, false, streamIdentifier, true, Long.MAX_VALUE,
                System.currentTimeMillis(), null);
    }

    //TODO: too long parameter
    public SparkExecutorPreAggFunction(Accumulator<Long> scannedRecords, Accumulator<Long> collectedRecords,
            String realizationType, //
            boolean isSplice, boolean hasPreFiltered, String streamIdentifier, boolean spillEnabled,
            long maxScannedBytes, long startTime, KryoTraceInfo kryoTraceInfo) {
        this.streamIdentifier = streamIdentifier;
        this.realizationType = realizationType;
        this.scannedRecords = scannedRecords;
        this.collectedRecords = collectedRecords;
        this.isSplice = isSplice;
        this.hasPreFiltered = hasPreFiltered;
        this.spillEnabled = spillEnabled;
        this.maxScannedBytes = maxScannedBytes;
        this.startTime = startTime;
        this.kryoTraceInfo = kryoTraceInfo;
    }

    @Override
    public Iterator<RDDPartitionResult> call(Iterator<Tuple2<Text, Text>> tuple2Iterator) throws Exception {

        HtraceInit.init();

        TraceScope scope = null;
        if (kryoTraceInfo != null) {
            scope = Trace.startSpan("per-partition executor task", kryoTraceInfo.toTraceInfo());
        }

        try {

            long localStartTime = System.currentTimeMillis();
            logger.info("Current stream identifier is {}", streamIdentifier);

            Iterator<ByteBuffer> iterator = Iterators.transform(tuple2Iterator,
                    new Function<Tuple2<Text, Text>, ByteBuffer>() {
                        @Nullable
                        @Override
                        public ByteBuffer apply(@Nullable Tuple2<Text, Text> input) {
                            return ByteBuffer.wrap(input._2.getBytes(), 0, input._2.getLength());
                        }
                    });

            GTScanRequest gtScanRequest = null;
            StorageSideBehavior behavior = null;

            ParquetBytesGTScanner scanner;
            if (RealizationType.CUBE.toString().equals(realizationType)) {
                if (isSplice) {
                    gtScanRequest = ParquetSpliceTarballFileInputFormat.ParquetTarballFileReader.gtScanRequestThreadLocal
                            .get();
                } else {
                    gtScanRequest = ParquetTarballFileInputFormat.ParquetTarballFileReader.gtScanRequestThreadLocal
                            .get();
                }
                behavior = StorageSideBehavior.valueOf(gtScanRequest.getStorageBehavior());
                scanner = new ParquetBytesGTScanner4Cube(gtScanRequest.getInfo(), iterator, gtScanRequest,
                        maxScannedBytes, behavior.delayToggledOn());//in
            } else if (RealizationType.INVERTED_INDEX.toString().equals(realizationType)) {
                gtScanRequest = ParquetRawTableFileInputFormat.ParquetRawTableFileReader.gtScanRequestThreadLocal.get();
                behavior = StorageSideBehavior.valueOf(gtScanRequest.getStorageBehavior());
                scanner = new ParquetBytesGTScanner4Raw(gtScanRequest.getInfo(), iterator, gtScanRequest,
                        maxScannedBytes, behavior.delayToggledOn());//in
            } else {
                throw new IllegalArgumentException("Unsupported realization type " + realizationType);
            }

            logger.info("Start latency is: {}", System.currentTimeMillis() - gtScanRequest.getStartTime());

            IGTScanner preAggred = gtScanRequest.decorateScanner(scanner, behavior.filterToggledOn(),
                    behavior.aggrToggledOn(), hasPreFiltered, spillEnabled);

            SparkExecutorGTRecordSerializer function = new SparkExecutorGTRecordSerializer(gtScanRequest,
                    gtScanRequest.getColumns());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Iterator<GTRecord> gtIterator = preAggred.iterator();
            long resultCounter = 0;
            while (gtIterator.hasNext()) {

                GTRecord row = gtIterator.next();
                ByteArray byteArray = function.apply(row);
                baos.write(byteArray.array(), 0, byteArray.length());

                resultCounter++;

                if (!gtScanRequest.isDoingStorageAggregation()) {
                    //if it's doing storage aggr, then should rely on GTAggregateScanner's limit check
                    if (gtScanRequest.getStorageLimitLevel() != StorageLimitLevel.NO_LIMIT
                            && resultCounter >= gtScanRequest.getStoragePushDownLimit()) {
                        //read one more record than limit
                        logger.info("The finalScanner aborted because storagePushDownLimit is satisfied");
                        break;
                    }
                }
            }

            baos.close();
            preAggred.close();

            logger.info("Current task scanned {} raw rows and {} raw bytes, contributing {} result rows",
                    scanner.getTotalScannedRowCount(), scanner.getTotalScannedRowBytes(), resultCounter);

            if (scannedRecords != null)
                scannedRecords.add(scanner.getTotalScannedRowCount());
            if (collectedRecords != null)
                collectedRecords.add(resultCounter);

            return Collections.singleton(new RDDPartitionResult(baos.toByteArray(), scanner.getTotalScannedRowCount(),
                    scanner.getTotalScannedRowBytes(), resultCounter, //
                    InetAddress.getLocalHost().getHostName(), localStartTime - startTime,
                    System.currentTimeMillis() - localStartTime)).iterator();
        } finally {
            if (scope != null)
                scope.close();
        }
    }
}
