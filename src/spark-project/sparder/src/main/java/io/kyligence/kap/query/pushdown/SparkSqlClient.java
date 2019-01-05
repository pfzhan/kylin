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

package io.kyligence.kap.query.pushdown;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkSqlClient implements Serializable {
    public static final Logger logger = LoggerFactory.getLogger(SparkSqlClient.class);
    public static final long serialVersionUID = 12L;
    private final transient SparkSession ss;
    private final transient Semaphore semaphore;
    private Map<UUID, Integer> uuidSizeMap = new HashMap<>();
    private final int maxDriverMem;
    private final int allocationTimeOut;

    public SparkSqlClient(SparkSession sc, Semaphore semaphore) {
        this.ss = sc;
        ss.sparkContext().setLocalProperty("spark.scheduler.pool", "query_pushdown");
        this.semaphore = semaphore;
        this.maxDriverMem = semaphore.availablePermits();
        this.allocationTimeOut = Strings
                .isNullOrEmpty(System.getProperty("kap.storage.columnar.driver-allocation-timeout")) ? 60
                        : Integer.parseInt(System.getProperty("kap.storage.columnar.driver-allocation-timeout"));
    }

    public Pair<List<List<String>>, List<StructField>> executeSql(String sql, UUID uuid) {
        String s = "Start to run sql with SparkSQL...";
        logger.info(s);
        Trace.addTimelineAnnotation(s);

        //Get result data
        Dataset<Row> df = ss.sql(sql);

        String msg = "SparkSQL returned result DataFrame";
        logger.info(msg);

        Trace.addTimelineAnnotation(msg);
        return DFToList(uuid, df);
    }

    private Pair<List<List<String>>, List<StructField>> DFToList(UUID uuid, Dataset<Row> df) {
        String jobGroup = Thread.currentThread().getName();
        ss.sparkContext().setJobGroup(jobGroup, "Push down: " , true);
        try {
            JavaRDD<List<String>> rowRdd = df.javaRDD().mapPartitions(new FlatMapFunctionImpl());

            //Get struct fields
            List<List<String>> sampleList;

            sampleList = rowRdd.takeSample(true, 1);

            long sampleLen = 0;

            if (!sampleList.isEmpty()) {
                for (String elem : sampleList.get(0)) {
                    if (elem != null) {
                        sampleLen += elem.getBytes().length;
                    }
                }
            }

            int estimateSize = (int) Math.ceil((double) (rowRdd.count() * sampleLen) / (1024 * 1024));
            uuidSizeMap.put(uuid, estimateSize);

            if (estimateSize > this.maxDriverMem) {
                throw new RuntimeException("Estimate size exceeds the maximum driver memory size");
            }

            /*try {
                if (!this.semaphore.tryAcquire(estimateSize, allocationTimeOut, TimeUnit.SECONDS)) {
                    String format = String.format(
                            "spark driver allocation memory more than %s s," + "please increase driver memory or"
                                    + " set kap.storage.columnar.driver-allocation-timeout ",
                            allocationTimeOut);
                    throw new RuntimeException(format);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("interruptted", e);
            }*/

            logger.info("Estimate size of dataframe is " + estimateSize + "m.");

            List<org.apache.spark.sql.types.StructField> originFieldList = JavaConversions.seqAsJavaList(df.schema());
            List<StructField> fieldList = new ArrayList<>(originFieldList.size());

            for (org.apache.spark.sql.types.StructField field : originFieldList) {
                StructField.StructFieldBuilder builder = new StructField.StructFieldBuilder();
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

                fieldList.add(builder.createStructField());
            }
            List<List<String>> rowList = Lists.newArrayList();
            /*if (SparderEnv.isAsyncQuery()) {
                SparderEnv.getResultRef().set(true);
                SparderEnv.setDF(df);
                final String separator = SparderEnv.getSeparator();
                String path = KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(QueryContext.current().getProject()) + "/"
                        + QueryContext.current().getQueryId();
                JavaRDD<String> mapRdd = rowRdd.map(new Function<List<String>, String>() {
                    @Override
                    public String call(List<String> v1) {
                        return StringUtil.join(v1, separator);
                    }
                });
                if (KapConfig.getInstanceFromEnv().isAsyncResultRepartitionEnabled()) {
                    mapRdd.repartition(SparderEnv.getAsyncResultCore()).saveAsTextFile(path);
                } else {
                    mapRdd.saveAsTextFile(path);
                }
            } else {
                rowList = rowRdd.collect();
            }*/
            rowList = rowRdd.collect();
            rowRdd.unpersist();
            df.unpersist();
            return Pair.newPair(rowList, fieldList);

        } catch (Throwable e) {
            if (e instanceof InterruptedException) {
                ss.sparkContext().cancelJobGroup(jobGroup);
                logger.info("Query timeout ", e);
                Thread.currentThread().interrupt();
                throw new KylinTimeoutException(
                        "Query timeout after: " + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds() + "s");
            } else {
                throw e;
            }
        }
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
