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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.kylin.common.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import scala.collection.JavaConversions;

public class SparkSqlClient implements Serializable {
    public static final Logger logger = LoggerFactory.getLogger(SparkSqlClient.class);

    private final transient JavaSparkContext sc;
    private final transient Semaphore semaphore;
    private HiveContext hiveContext;
    private Map<UUID, Integer> uuidSizeMap = new HashMap<>();

    public SparkSqlClient(JavaSparkContext sc, Semaphore semaphore) {
        this.sc = sc;
        this.semaphore = semaphore;
        hiveContext = new HiveContext(sc);
    }

    public Pair<List<List<String>>, List<SparkJobProtos.StructField>> executeSql(SparkJobProtos.AdHocRequest request,
            UUID uuid) throws Exception {
        logger.info("Start to run sql with Spark <<<<<<");

        try {
            //Get result data
            DataFrame df = hiveContext.sql(request.getSql());

            JavaRDD<List<String>> rowRdd = df.javaRDD()
                    .mapPartitions(new FlatMapFunction<Iterator<Row>, List<String>>() {

                        @Override
                        public Iterable<List<String>> call(Iterator<Row> iterator) throws Exception {
                            List<List<String>> rowList = new ArrayList<>();

                            while (iterator.hasNext()) {
                                List<String> data = Lists.newArrayList();
                                Row curRow = iterator.next();

                                for (int i = 0; i < curRow.length(); i++) {
                                    Object obj = curRow.getAs(i);
                                    if (null == obj) {
                                        data.add("");
                                    } else {
                                        data.add(obj.toString());
                                    }
                                }
                                rowList.add(data);
                            }
                            return rowList;
                        }
                    });

            //Get struct fields

            List<List<String>> sampleList = rowRdd.takeSample(true, 1);
            long sampleLen = 0;

            if (!sampleList.isEmpty()) {
                for (String elem : sampleList.get(0)) {
                    sampleLen += elem.getBytes().length;
                }
            }

            int estimateSize = (int) Math.ceil((double) (rowRdd.count() * sampleLen) / (1024 * 1024));
            uuidSizeMap.put(uuid, estimateSize);

            this.semaphore.acquire(estimateSize);
            logger.info("Estimate size of dataframe is " + estimateSize + "m.");

            List<StructField> originFieldList = JavaConversions.asJavaList(df.schema().toList());
            List<SparkJobProtos.StructField> fieldList = new ArrayList<>(originFieldList.size());

            for (StructField field : originFieldList) {
                SparkJobProtos.StructField.Builder builder = SparkJobProtos.StructField.newBuilder();
                builder.setName(field.name());
                builder.setDataType(field.dataType().toString());
                builder.setNullable(field.nullable());
                fieldList.add(builder.build());
            }

            List<List<String>> rowList = rowRdd.collect();

            rowRdd.unpersist();
            df.unpersist();

            return Pair.newPair(rowList, fieldList);

        } catch (Exception e) {
            logger.error("Ad Hoc Query Error:", e);
            throw new StatusRuntimeException(Status.INTERNAL
                    .withDescription("Ad hoc query not supported, please check spark-driver.log for details."));
        }
    }

    public long getEstimateDfSize(UUID uuid) {
        return uuidSizeMap.remove(uuid);
    }
}
