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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import io.kyligence.kap.ext.classloader.ClassLoaderUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.storage.gtrecord.IPartitionStreamer;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;

public class SparkSubmitter {
    static SparkSqlClient sqlClient = null;

    public static IPartitionStreamer submitParquetTask(GTScanRequest scanRequest,
            SparkJobProtos.SparkJobRequestPayload payload, long queryMaxScanBytes) {
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        final String scanReqId = Integer.toHexString(System.identityHashCode(scanRequest));
        final String streamIdentifier = UUID.randomUUID().toString();// it will stay during the stream session
        ParquetTask parquetTask = new ParquetTask(payload, streamIdentifier);
        return new KyStorageVisitStreamer(Iterators.concat(new ParquetResultIterator(parquetTask)), scanReqId,
                queryMaxScanBytes);
    }

    public static SparkJobProtos.PushDownResponse submitPushDownTask(SparkJobProtos.PushDownRequest request) {
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        if (sqlClient == null) {
            sqlClient = new SparkSqlClient(new Semaphore((int) (Runtime.getRuntime().totalMemory() / (1024 * 1024))));
        }
        Pair<List<List<String>>, List<SparkJobProtos.StructField>> pair = null;
        try {
            pair = sqlClient.executeSql(request, UUID.randomUUID());
        } catch (Exception e) {
            throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getLocalizedMessage()));
        }
        return SparkJobProtos.PushDownResponse.newBuilder()
                .addAllRows(Iterables.transform(pair.getFirst(), new Function<List<String>, SparkJobProtos.Row>() {
                    @Nullable
                    @Override
                    public SparkJobProtos.Row apply(@Nullable List<String> input) {
                        SparkJobProtos.Row.Builder rowBuilder = SparkJobProtos.Row.newBuilder();

                        if (input != null) {
                            for (String elem : input) {
                                SparkJobProtos.Row.Cell.Builder dataBuilder = SparkJobProtos.Row.Cell.newBuilder();

                                if (elem != null) {
                                    dataBuilder.setValue(elem);
                                }
                                rowBuilder.addCell(dataBuilder.build());
                            }
                        }

                        return rowBuilder.build();
                    }
                })).addAllColumns(pair.getSecond()).build();
    }

}
