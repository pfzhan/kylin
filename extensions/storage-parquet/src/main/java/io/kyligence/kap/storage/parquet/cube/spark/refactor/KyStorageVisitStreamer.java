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

package io.kyligence.kap.storage.parquet.cube.spark.refactor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.kylin.storage.gtrecord.IPartitionStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;

public class KyStorageVisitStreamer implements IPartitionStreamer {
    public static final Logger logger = LoggerFactory.getLogger(KyStorageVisitStreamer.class);

    Iterator<SparkJobProtos.SparkJobResponse.PartitionResponse> iterator;
    String scanRequestId;
    long queryMaxScanBytes;

    public KyStorageVisitStreamer(Iterator<SparkJobProtos.SparkJobResponse.PartitionResponse> iterator,
            String scanRequestId, long queryMaxScanBytes) {
        this.iterator = iterator;
        this.scanRequestId = scanRequestId;
        this.queryMaxScanBytes = queryMaxScanBytes;
    }

    @Override
    public Iterator<byte[]> asByteArrayIterator() {

        final Iterator<byte[]> itr = Iterators.transform(iterator,
                new Function<SparkJobProtos.SparkJobResponse.PartitionResponse, byte[]>() {
                    @Override
                    public byte[] apply(@Nullable SparkJobProtos.SparkJobResponse.PartitionResponse partitionResponse) {

                        byte[] bytes = partitionResponse.getBlob().toByteArray();
                        logger.info(
                                "[Partition Response Metrics] scan-request %s, result bytes: %s, scanned rows: {}, scanned bytes: {},  returned rows: {} , start latency: {}, partition duration: {}, partition calculated on {}",
                                //
                                scanRequestId, bytes.length, partitionResponse.getScannedRows(),
                                partitionResponse.getScannedBytes(), partitionResponse.getReturnedRows(),
                                partitionResponse.getStartLatency(), partitionResponse.getTotalDuration(),
                                partitionResponse.getHostname());

                        QueryContext.current().addAndGetScannedRows(partitionResponse.getScannedRows());
                        QueryContext.current().addAndGetScannedBytes(partitionResponse.getScannedBytes());

                        if (QueryContext.current().getScannedBytes() > queryMaxScanBytes) {
                            throw new ResourceLimitExceededException(
                                    "Query scanned " + QueryContext.current().getScannedBytes()
                                            + " bytes exceeds threshold " + queryMaxScanBytes);
                        }
                        //only for debug/profile purpose
                        if (BackdoorToggles.getPartitionDumpDir() != null) {
                            logger.info("debugging: Dumping partitions");
                            dumpPartitions(bytes);
                        }

                        return bytes;
                    }
                });

        //for tracing
        return new Iterator<byte[]>() {
            int hasNextTimes = 0;
            int nextTimes = 0;

            @Override
            public boolean hasNext() {
                Trace.addTimelineAnnotation("calcite asking for one more partition result, cardinal within seg: " + (hasNextTimes++));
                return itr.hasNext();
            }

            @Override
            public byte[] next() {
                byte[] next = itr.next();
                Trace.addTimelineAnnotation("one more partition result to calcite, cardinal within seg: " + (nextTimes++));
                return next;
            }

            @Override
            public void remove() {
                itr.remove();
            }
        };
    }

    private void dumpPartitions(byte[] bytes) {
        File dir = new File(BackdoorToggles.getPartitionDumpDir());
        if (!dir.exists()) {
            dir.mkdirs();
        }

        if (dir.exists() && dir.isDirectory()) {
            int numFiles = dir.listFiles().length;
            File f = new File(dir, String.valueOf(numFiles));
            try (FileOutputStream fileOutputStream = new FileOutputStream(f, false)) {
                IOUtils.write(bytes, fileOutputStream);
            } catch (Exception e) {
                logger.error("error", e);
            }
        } else {
            logger.error("BackdoorToggles.getPartitionDumpDir() not valid dir for dumping");
        }

    }

    @Override
    public void close() throws IOException {

    }
}
