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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.kyligence.kap.common.obf.IKeepClassMembers;
import io.kyligence.kap.storage.parquet.adhoc.SparkSqlClient;
import io.kyligence.kap.storage.parquet.adhoc.util.KapPushDownUtil;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;

public class SparkAppClientService extends JobServiceGrpc.JobServiceImplBase implements IKeepClassMembers {

    public static final Logger logger = LoggerFactory.getLogger(SparkAppClientService.class);

    private SparkConf conf;
    private JavaSparkContext sc;
    private Semaphore semaphore;
    private SparkSqlClient sqlClient;

    public SparkAppClientService() {
        conf = new SparkConf().setAppName("KyStorage for Kyligence Analytical Platform");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "false");

        int coreNum = Strings.isNullOrEmpty(System.getProperty("kap.storage.columnar.spark-conf.spark.executor.cores"))
                ? 1 : Integer.parseInt(System.getProperty("kap.storage.columnar.spark-conf.spark.executor.cores"));
        int instanceNum = Strings
                .isNullOrEmpty(System.getProperty("kap.storage.columnar.spark-conf.spark.executor.instances")) ? 1
                        : Integer.parseInt(
                                System.getProperty("kap.storage.columnar.spark-conf.spark.executor.instances"));

        conf.set("spark.sql.shuffle.partitions", String.valueOf(coreNum * instanceNum));
        conf.registerKryoClasses(new Class[] { scala.collection.mutable.WrappedArray.ofRef.class, Object[].class,
                RDDPartitionResult.class, SparkExecutorPreAggFunction.class });

        //for spark sql
        //https://mail-archives.apache.org/mod_mbox/spark-user/201603.mbox/%3CCAHCfvsSyUpx78ZFS_A9ycxvtO1=Jp7DfCCAeJKHyHZ1sugqHEQ@mail.gmail.com%3E
        try {
            conf.registerKryoClasses(new Class[] { org.apache.spark.sql.types.StructType.class,
                    org.apache.spark.sql.types.StructField.class, org.apache.spark.sql.types.StructField[].class,
                    org.apache.spark.sql.types.LongType.class, org.apache.spark.sql.types.Metadata.class,
                    org.apache.spark.sql.catalyst.InternalRow.class, org.apache.spark.sql.catalyst.InternalRow[].class,
                    org.apache.spark.sql.catalyst.expressions.SortOrder.class,
                    org.apache.spark.sql.catalyst.expressions.Literal.class,
                    org.apache.spark.sql.catalyst.expressions.GenericInternalRow.class,
                    org.apache.spark.sql.catalyst.expressions.UnsafeRow.class,
                    org.apache.spark.sql.catalyst.expressions.UnsafeRow[].class,
                    org.apache.spark.sql.catalyst.expressions.InterpretedOrdering.class,
                    org.apache.spark.sql.execution.joins.UnsafeHashedRelation.class,
                    org.apache.spark.sql.catalyst.trees.Origin.class, java.util.HashMap.class,
                    org.apache.spark.sql.catalyst.expressions.Descending.class,
                    Class.forName("scala.math.Ordering$$anon$4"),
                    Class.forName("org.apache.spark.sql.types.IntegerType$"),
                    Class.forName("scala.reflect.ClassTag$$anon$1"), Class.class,
                    org.apache.spark.unsafe.types.UTF8String.class,
                    org.apache.spark.sql.execution.columnar.CachedBatch.class, byte[][].class, ArrayList.class });
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        sc = new JavaSparkContext(conf);
        semaphore = new Semaphore((int) KapPushDownUtil.memoryStringToMegas(this.conf.get("spark.driver.memory")) / 2);
        sqlClient = new SparkSqlClient(sc, semaphore);

        logger.info("Starting to warm up all executors");
        List<Integer> warmupData = new ArrayList<Integer>();
        for (int i = 0; i < 10000; i++) {
            warmupData.add(i);
        }
        sc.parallelize(warmupData).count();
        logger.info("Finish warming up all executors");
    }

    private void checkSparkContext() {
        if (sc.sc().isStopped()) {
            synchronized (SparkAppClientService.class) {
                if (sc.sc().isStopped()) {
                    logger.warn(
                            "Current JavaSparkContext(started at {} GMT) is found to be stopped, creating a new one",
                            DateFormat.formatToTimeStr(sc.startTime()));
                    sc = new JavaSparkContext(conf);
                    semaphore = new Semaphore(
                            (int) KapPushDownUtil.memoryStringToMegas(this.conf.get("spark.driver.memory")) / 2);
                    sqlClient = new SparkSqlClient(sc, semaphore);
                }
            }
        }
    }

    @Override
    public StreamObserver<SparkJobRequest> submitJob(final StreamObserver<SparkJobResponse> responseObserver) {
        checkSparkContext();
        return new ServerStreamObserver(responseObserver, sc);
    }

    @Override
    public void doPushDownQuery(SparkJobProtos.PushDownRequest request,
            StreamObserver<SparkJobProtos.PushDownResponse> responseObserver) {
        checkSparkContext();

        logger.info("Starting to do query push down");
        try {
            UUID uuid = UUID.randomUUID();
            Pair<List<List<String>>, List<SparkJobProtos.StructField>> pair = sqlClient.executeSql(request, uuid);

            responseObserver.onNext(SparkJobProtos.PushDownResponse.newBuilder()
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
                    })).addAllColumns(pair.getSecond()).build());
            responseObserver.onCompleted();

            semaphore.release((int) sqlClient.getEstimateDfSize(uuid));

        } catch (Exception e) {
            logger.error("Query Push Down Error:", e);
            throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getLocalizedMessage()));
        }
    }
}
