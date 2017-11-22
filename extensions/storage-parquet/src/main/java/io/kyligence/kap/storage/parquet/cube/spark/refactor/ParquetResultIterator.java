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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.Pair;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.RDDPartitionResult;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.protocol.shaded.com.google.protobuf.ByteString;

public class ParquetResultIterator
        implements Closeable, Iterator<Iterator<SparkJobProtos.SparkJobResponse.PartitionResponse>> {
    private ParquetTask parquetTask;
    private JavaRDD<RDDPartitionResult> rddHandle = null;

    public ParquetResultIterator(ParquetTask parquetTask) {
        this.parquetTask = parquetTask;
    }

    @Override
    public boolean hasNext() {
        return parquetTask.moreRDDExists();
    }

    @Override
    public Iterator<SparkJobProtos.SparkJobResponse.PartitionResponse> next() {
        Pair<Iterator<RDDPartitionResult>, JavaRDD<RDDPartitionResult>> iteratorJavaRDDPair = null;
        try {
            iteratorJavaRDDPair = parquetTask.executeTask();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        rddHandle = iteratorJavaRDDPair.getSecond();

        Iterator<SparkJobProtos.SparkJobResponse.PartitionResponse> sparkJobResponseIterator = Iterators.transform(
                iteratorJavaRDDPair.getFirst(),
                new Function<RDDPartitionResult, SparkJobProtos.SparkJobResponse.PartitionResponse>() {

                    @Nullable
                    @Override
                    public SparkJobProtos.SparkJobResponse.PartitionResponse apply(
                            @Nullable RDDPartitionResult resultPartition) {
                        return SparkJobProtos.SparkJobResponse.PartitionResponse.newBuilder().//
                        setBlob(ByteString.copyFrom(resultPartition.getData())).//
                        setScannedRows(resultPartition.getScannedRows()).//
                        setScannedBytes(resultPartition.getScannedBytes()).//
                        setReturnedRows(resultPartition.getReturnedRows()).//
                        setHostname(resultPartition.getHostname()).//
                        setTotalDuration(resultPartition.getTotalDuration()).//
                        setStartLatency(resultPartition.getStartLatency()).//
                        build();
                    }
                });
        return sparkJobResponseIterator;

    }

    @Override
    public void remove() {

    }

    @Override
    public void close() throws IOException {
        if (rddHandle != null) {
            rddHandle.unpersist();
        }
    }
}
