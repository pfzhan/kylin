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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
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
    private final int maxDriverMem;
    private final int allocationTimeOut;

    public SparkSqlClient(JavaSparkContext sc, Semaphore semaphore) {
        this.sc = sc;
        sc.setLocalProperty("spark.scheduler.pool", "query_pushdown");
        this.semaphore = semaphore;
        this.maxDriverMem = semaphore.availablePermits();
        this.allocationTimeOut = Strings
                .isNullOrEmpty(System.getProperty("kap.storage.columnar.driver-allocation-timeout")) ? 60
                        : Integer.parseInt(System.getProperty("kap.storage.columnar.driver-allocation-timeout"));
        hiveContext = new HiveContext(sc);
        hiveContext.sql(
                "CREATE TEMPORARY FUNCTION timestampadd AS 'io.kyligence.kap.storage.parquet.adhoc.udf.TimestampAdd'");
    }

    public Pair<List<List<String>>, List<SparkJobProtos.StructField>> executeSql(SparkJobProtos.PushDownRequest request,
            UUID uuid) throws Exception {
        logger.info("Start to run sql with Spark <<<<<<");

        try {
            //Get result data
            Dataset<Row> df = hiveContext.sql(request.getSql());

            JavaRDD<List<String>> rowRdd = df.javaRDD()
                    .mapPartitions(new FlatMapFunction<Iterator<Row>, List<String>>() {

                        @Override
                        public Iterator<List<String>> call(Iterator<Row> iterator) throws Exception {
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
                            return rowList.iterator();
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

            if (estimateSize > this.maxDriverMem) {
                throw new RuntimeException("Estimate size exceeds the maximum driver memory size");
            }

            if (!this.semaphore.tryAcquire(estimateSize, allocationTimeOut, TimeUnit.SECONDS)) {
                throw new RuntimeException(String.format("spark driver allocation memory more than %s s,"
                        + "please increase driver memory or" + " set kap.storage.columnar.driver-allocation-timeout ",
                        allocationTimeOut));
            }

            logger.info("Estimate size of dataframe is " + estimateSize + "m.");

            List<StructField> originFieldList = JavaConversions.seqAsJavaList(df.schema());
            List<SparkJobProtos.StructField> fieldList = new ArrayList<>(originFieldList.size());

            for (StructField field : originFieldList) {
                SparkJobProtos.StructField.Builder builder = SparkJobProtos.StructField.newBuilder();
                builder.setName(field.name());

                int type = Types.OTHER;
                String typeName = field.dataType().sql();

                switch (typeName) {
                case "BINARY":
                    type = Types.BINARY;
                    break;
                case "BOOLEAN":
                    type = Types.BOOLEAN;
                    break;
                case "DATE":
                    type = Types.DATE;
                    break;
                case "DOUBLE":
                    type = Types.DOUBLE;
                    break;
                case "FLOAT":
                    type = Types.FLOAT;
                    break;
                case "INT":
                    type = Types.INTEGER;
                    break;
                case "BIGINT":
                    type = Types.BIGINT;
                    break;
                case "NUMERIC":
                    type = Types.NUMERIC;
                    break;
                case "SMALLINT":
                    type = Types.SMALLINT;
                    break;
                case "TIMESTAMP":
                    type = Types.TIMESTAMP;
                    break;
                case "STRING":
                    type = Types.VARCHAR;
                    break;
                default:
                    break;
                }

                if (typeName.startsWith("DECIMAL")) {
                    Pair<Integer, Integer> precisionAndScalePair = getDecimalPrecisionAndScale(typeName);

                    if (precisionAndScalePair != null) {
                        builder.setPrecision(precisionAndScalePair.getFirst());
                        builder.setScale(precisionAndScalePair.getSecond());
                    }
                    type = Types.DECIMAL;
                    typeName = "DECIMAL";
                }
                builder.setDataType(type);
                builder.setDataTypeName(typeName);
                builder.setNullable(field.nullable());

                if (!df.resolve(field.name()).qualifier().isEmpty()) {
                    String tableName = df.resolve(field.name()).qualifier().get();
                    builder.setTable(tableName);
                }

                fieldList.add(builder.build());
            }

            List<List<String>> rowList = rowRdd.collect();

            rowRdd.unpersist();
            df.unpersist();

            return Pair.newPair(rowList, fieldList);

        } catch (Exception e) {
            logger.error("Query Push Down Error:", e);
            throw new StatusRuntimeException(
                    Status.INTERNAL.withDescription("Query push down failed with exception message: " + e.getMessage()
                            + ", please check spark-driver.log for details."));
        }
    }

    public long getEstimateDfSize(UUID uuid) {
        return uuidSizeMap.remove(uuid);
    }

    private Pair<Integer, Integer> getDecimalPrecisionAndScale(String type) {
        Pattern DECIMAL_PATTERN = Pattern.compile("DECIMAL\\(([0-9]+),([0-9]+)\\)", Pattern.CASE_INSENSITIVE);
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(type);
        if (decimalMatcher.find()) {
            int precision = Integer.valueOf(decimalMatcher.group(1));
            int scale = Integer.valueOf(decimalMatcher.group(2));

            return new Pair<>(precision, scale);
        } else {
            return null;
        }
    }
}
