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

package io.kyligence.kap.storage.parquet.adhoc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

public class SparkSqlClient implements Serializable {
    public static final Logger logger = LoggerFactory.getLogger(SparkSqlClient.class);

    private final transient JavaSparkContext sc;
    private final transient SparkJobProtos.AdHocRequest request;
    private final transient Semaphore semaphore;
    private long estimateDfSize;

    public SparkSqlClient(JavaSparkContext sc, SparkJobProtos.AdHocRequest request, Semaphore semaphore) {
        this.sc = sc;
        this.request = request;
        this.semaphore = semaphore;
        this.estimateDfSize = 0;
    }

    public Pair<List<SparkJobProtos.Row>, List<SparkJobProtos.StructField>> executeSql() throws Exception {
        logger.info("Start to run sql with Spark <<<<<<");

        try {
            HiveContext hiveContext = new HiveContext(sc);

            //Get result data
            DataFrame df = hiveContext.sql(request.getSql());
            estimateDfSize = SizeEstimator.estimate(df) / (1024 * 1024);
            logger.info("Estimate size of dataframe is " + estimateDfSize + "m.");

            this.semaphore.acquire((int) estimateDfSize);

            JavaRDD<SparkJobProtos.Row> rowRdd = df.javaRDD().mapPartitions(new FlatMapFunction<Iterator<Row>, SparkJobProtos.Row>() {

                @Override
                public Iterable<SparkJobProtos.Row> call(Iterator<Row> iterator) throws Exception {
                    List<SparkJobProtos.Row> rowList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        SparkJobProtos.Row.Builder builder = SparkJobProtos.Row.newBuilder();
                        Row curRow = iterator.next();

                        for (int i = 0; i < curRow.length(); i++) {
                            builder.addData(curRow.getAs(i).toString());
                        }
                        rowList.add(builder.build());
                    }
                    return rowList;
                }
            });

            //Get struct fields
            List<StructField> originFieldList = JavaConversions.asJavaList(df.schema().toList());
            List<SparkJobProtos.StructField> fieldList = new ArrayList<>(originFieldList.size());

            for(StructField field: originFieldList) {
                SparkJobProtos.StructField.Builder builder = SparkJobProtos.StructField.newBuilder();
                builder.setName(field.name());
                builder.setDataType(field.dataType().toString());
                builder.setNullable(field.nullable());
                fieldList.add(builder.build());
            }

            List<SparkJobProtos.Row> rowList = rowRdd.collect();

            return Pair.newPair(rowList, fieldList);

        } catch (Exception e) {
            logger.error("Ad Hoc Query Error:", e);
            throw new StatusRuntimeException(Status.INTERNAL.withDescription("Ad hoc query not supported, please check spark-driver.log for details."));
        }
    }

    public long getEstimateDfSize() {
        return estimateDfSize;
    }
}

