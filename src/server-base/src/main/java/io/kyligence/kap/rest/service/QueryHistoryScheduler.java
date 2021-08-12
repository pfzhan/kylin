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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;

public class QueryHistoryScheduler {

    protected BlockingQueue<QueryMetrics> queryMetricsQueue;

    private static final Logger logger = LoggerFactory.getLogger("query");

    private ScheduledExecutorService writeQueryHistoryScheduler;

    public void init() throws Exception {
        writeQueryHistoryScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("WriteQueryHistoryWorker"));
        KylinConfig kyinConfig = KylinConfig.getInstanceFromEnv();
        writeQueryHistoryScheduler.scheduleWithFixedDelay(new WriteQueryHistoryRunner(), 1,
                kyinConfig.getQueryHistorySchedulerInterval(), TimeUnit.SECONDS);
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
                queryHistoryDAO.insert(metrics);
            } catch (Throwable th) {
                logger.error("Error when write query history", th);
            }
        }

    }

}
