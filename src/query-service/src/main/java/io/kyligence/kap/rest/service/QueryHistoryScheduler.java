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

package io.kyligence.kap.rest.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.query.util.SparkJobTrace;
import io.kyligence.kap.query.util.SparkJobTraceMetric;

public class QueryHistoryScheduler {

    protected BlockingQueue<QueryMetrics> queryMetricsQueue;

    private static final Logger logger = LoggerFactory.getLogger("query");

    private ScheduledExecutorService writeQueryHistoryScheduler;

    private long sparkJobTraceTimeoutMs;
    private boolean isQuerySparkJobTraceEnabled;
    private boolean isSecondStorageQueryMetricCollect;

    public void init() throws Exception {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        sparkJobTraceTimeoutMs = kapConfig.getSparkJobTraceTimeoutMs();
        isQuerySparkJobTraceEnabled = kapConfig.isQuerySparkJobTraceEnabled();
        writeQueryHistoryScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("WriteQueryHistoryWorker"));
        KylinConfig kyinConfig = KylinConfig.getInstanceFromEnv();
        writeQueryHistoryScheduler.scheduleWithFixedDelay(new WriteQueryHistoryRunner(), 1,
                kyinConfig.getQueryHistorySchedulerInterval(), TimeUnit.SECONDS);
        isSecondStorageQueryMetricCollect = KylinConfig.getInstanceFromEnv().getSecondStorageQueryMetricCollect();
    }

    public static QueryHistoryScheduler getInstance() {
        return Singletons.getInstance(QueryHistoryScheduler.class);
    }

    public QueryHistoryScheduler() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        queryMetricsQueue = new LinkedBlockingQueue<>(kylinConfig.getQueryHistoryBufferSize());
        logger.debug("New NQueryHistoryScheduler created");
    }

    public void offerQueryHistoryQueue(QueryMetrics queryMetrics) {
        boolean offer = queryMetricsQueue.offer(queryMetrics);
        if (!offer) {
            logger.info("queryMetricsQueue is full");
        }
    }

    synchronized void shutdown() {
        logger.info("Shutting down NQueryHistoryScheduler ....");
        if (writeQueryHistoryScheduler != null) {
            ExecutorServiceUtil.forceShutdown(writeQueryHistoryScheduler);
        }
    }

    public class WriteQueryHistoryRunner implements Runnable {

        RDBMSQueryHistoryDAO queryHistoryDAO;

        WriteQueryHistoryRunner() {
            queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        }

        @Override
        public void run() {
            try {
                List<QueryMetrics> metrics = Lists.newArrayList();
                queryMetricsQueue.drainTo(metrics);
                List<QueryMetrics> insertMetrics;
                if (isQuerySparkJobTraceEnabled && metrics.size() > 0) {
                    insertMetrics = metrics.stream().filter(queryMetrics -> {
                        String queryId = queryMetrics.getQueryId();
                        SparkJobTraceMetric sparkJobTraceMetric = SparkJobTrace.getSparkJobTraceMetric(queryId);
                        return isCollectedFinished(queryId, sparkJobTraceMetric, queryMetrics);
                    }).collect(Collectors.toList());
                } else {
                    insertMetrics = metrics;
                }
                collectSecondStorageMetric(insertMetrics);
                queryHistoryDAO.insert(insertMetrics);
            } catch (Exception th) {
                logger.error("Error when write query history", th);
            }
        }

    }

    public boolean isCollectedFinished(String queryId, SparkJobTraceMetric sparkJobTraceMetric,
            QueryMetrics queryMetrics) {
        if (sparkJobTraceMetric != null) {
            // queryHistoryInfo collect asynchronous metrics
            List<QueryHistoryInfo.QueryTraceSpan> queryTraceSpans = queryMetrics.getQueryHistoryInfo().getTraces();
            AtomicLong timeCostSum = new AtomicLong(0);
            queryTraceSpans.forEach(span -> {
                if (QueryTrace.PREPARE_AND_SUBMIT_JOB.equals(span.getName())) {
                    span.setDuration(sparkJobTraceMetric.getPrepareAndSubmitJobMs());
                }
                timeCostSum.addAndGet(span.getDuration());
            });
            queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.WAIT_FOR_EXECUTION,
                    QueryTrace.SPAN_GROUPS.get(QueryTrace.WAIT_FOR_EXECUTION),
                    sparkJobTraceMetric.getWaitForExecutionMs()));
            timeCostSum.addAndGet(sparkJobTraceMetric.getWaitForExecutionMs());
            queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.EXECUTION,
                    QueryTrace.SPAN_GROUPS.get(QueryTrace.EXECUTION), sparkJobTraceMetric.getExecutionMs()));
            timeCostSum.addAndGet(sparkJobTraceMetric.getExecutionMs());
            queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.FETCH_RESULT,
                    QueryTrace.SPAN_GROUPS.get(QueryTrace.FETCH_RESULT),
                    queryMetrics.getQueryDuration() - timeCostSum.get()));
            return true;
        } else if ((System.currentTimeMillis()
                - (queryMetrics.getQueryTime() + queryMetrics.getQueryDuration())) > sparkJobTraceTimeoutMs) {
            logger.warn(
                    "QueryMetrics timeout lost spark job trace kylin.query.spark-job-trace-timeout-ms={} queryId:{}",
                    sparkJobTraceTimeoutMs, queryId);
            return true;
        } else {
            offerQueryHistoryQueue(queryMetrics);
            return false;
        }
    }

    public void collectSecondStorageMetric(List<QueryMetrics> metrics) {
        if (!isSecondStorageQueryMetricCollect) {
            return;
        }

        if (!SecondStorageUtil.isGlobalEnable()) {
            return;
        }

        SecondStorageUpdater updater = SpringContext.getBean(SecondStorageUpdater.class);

        for (QueryMetrics metric : metrics) {
            try {
                if (metric.isSecondStorage() && SecondStorageUtil.isProjectEnable(metric.getProjectName())) {
                    Map<String, Object> secondStorageMetrics = updater.getQueryMetric(metric.getProjectName(), metric.getQueryId());

                    if (secondStorageMetrics.containsKey(QueryMetrics.TOTAL_SCAN_BYTES)) {
                        metric.setTotalScanBytes((long) secondStorageMetrics.get(QueryMetrics.TOTAL_SCAN_BYTES));
                    }

                    if (secondStorageMetrics.containsKey(QueryMetrics.TOTAL_SCAN_COUNT)) {
                        metric.setTotalScanCount((long) secondStorageMetrics.get(QueryMetrics.TOTAL_SCAN_COUNT));
                    }
                }
            } catch (Exception e) {
                logger.error("Get tired storage metric fail. query_id: {}, message: {}", metric.getQueryId(), e.getMessage());
            }
        }
    }
}
