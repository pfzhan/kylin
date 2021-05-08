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
package io.kyligence.kap.rest.monitor;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kylin.common.KapConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import lombok.Getter;
import lombok.val;

public class SparkContextCanary {
    private static final Logger logger = LoggerFactory.getLogger(SparkContextCanary.class);
    private static volatile boolean isStarted = false;

    private static final int THRESHOLD_TO_RESTART_SPARK = KapConfig.getInstanceFromEnv().getThresholdToRestartSpark();
    private static final int PERIOD_MINUTES = KapConfig.getInstanceFromEnv().getSparkCanaryPeriodMinutes();
    private static final String WORKING_DIR = KapConfig.getInstanceFromEnv().getWriteHdfsWorkingDirectory();

    @Getter
    private static volatile int errorAccumulated = 0;
    @Getter
    private static volatile long lastResponseTime = -1;
    @Getter
    private static volatile boolean sparkRestarting = false;

    private SparkContextCanary() {
    }

    public static void init() {
        if (!isStarted) {
            synchronized (SparkContextCanary.class) {
                if (!isStarted) {
                    isStarted = true;
                    logger.info("Start monitoring Spark");
                    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(SparkContextCanary::monitor,
                            PERIOD_MINUTES, PERIOD_MINUTES, TimeUnit.MINUTES);
                }
            }
        }
    }

    public static boolean isError() {
        return errorAccumulated >= THRESHOLD_TO_RESTART_SPARK;
    }

    static void monitor() {
        try {
            long startTime = System.currentTimeMillis();
            // check spark sql context
            if (!SparderEnv.isSparkAvailable()) {
                logger.info("Spark is unavailable, need to restart immediately.");
                errorAccumulated = Math.max(errorAccumulated + 1, THRESHOLD_TO_RESTART_SPARK);
            } else {
                val jsc = JavaSparkContext.fromSparkContext(SparderEnv.getSparkSession().sparkContext());
                Future<Boolean> handler = checkWriteFile(jsc);
                try {
                    jsc.setLocalProperty("spark.scheduler.pool", "vip_tasks");

                    long t = System.currentTimeMillis();
                    handler.get(KapConfig.getInstanceFromEnv().getSparkCanaryErrorResponseMs(), TimeUnit.MILLISECONDS);
                    logger.info("SparkContextCanary checkWriteFile returned successfully, takes {} ms.",
                            (System.currentTimeMillis() - t));
                    // reset errorAccumulated once good context is confirmed
                    errorAccumulated = 0;
                } catch (TimeoutException te) {
                    errorAccumulated++;
                    handler.cancel(true);
                    logger.error("SparkContextCanary write file timeout.", te);
                } catch (InterruptedException e) {
                    errorAccumulated++;
                    logger.error("Thread is interrupted.", e);
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    errorAccumulated = Math.max(errorAccumulated + 1, THRESHOLD_TO_RESTART_SPARK);
                    logger.error("SparkContextCanary numberCount occurs exception, need to restart immediately.", e);
                } catch (Exception e) {
                    errorAccumulated++;
                    logger.error("SparkContextCanary write file occurs exception.", e);
                }
            }

            lastResponseTime = System.currentTimeMillis() - startTime;
            logger.debug("Spark context errorAccumulated:{}", errorAccumulated);

            if (isError()) {
                sparkRestarting = true;
                try {
                    // Take repair action if error accumulated exceeds threshold
                    logger.warn("Repairing spark context");
                    SparderEnv.restartSpark();

                    MetricsGroup.hostTagCounterInc(MetricsName.SPARDER_RESTART, MetricsCategory.GLOBAL, "global");
                } catch (Throwable th) {
                    logger.error("Restart spark context failed.", th);
                }
                sparkRestarting = false;
            }
        } catch (Throwable th) {
            logger.error("Error when monitoring Spark.", th);
            Thread.currentThread().interrupt();
        }
    }

    // for canary
    private static Future<Boolean> checkWriteFile(JavaSparkContext jsc) {

        ExecutorService executor = Executors.newSingleThreadExecutor();

        final Future<Boolean> handler = executor.submit(new Callable() {
            @Override
            public Boolean call() throws Exception {
                val ss = SparderEnv.getSparkSession();
                val list = new ArrayList<Row>();
                for (int i = 0; i < 100; i++) {
                    list.add(RowFactory.create(i));
                }

                val schema = new StructType(
                        new StructField[] { DataTypes.createStructField("col", DataTypes.IntegerType, true) });
                val df = ss.createDataFrame(jsc.parallelize(list), schema);
                val appId = ss.sparkContext().applicationId();
                df.write().mode(SaveMode.Overwrite).parquet(WORKING_DIR + "/" + appId);
                return true;
            }
        });

        return handler;
    }
}
