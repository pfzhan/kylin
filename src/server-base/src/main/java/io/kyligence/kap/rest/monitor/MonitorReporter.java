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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.metrics.service.JobStatusMonitorMetric;
import io.kyligence.kap.common.metrics.service.MonitorDao;
import io.kyligence.kap.common.metrics.service.MonitorMetric;
import io.kyligence.kap.common.metrics.service.QueryMonitorMetric;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.rest.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MonitorReporter {
    private static final Logger logger = LoggerFactory.getLogger(MonitorReporter.class);

    private ScheduledExecutorService dataCollectorExecutor;
    private static final int MAX_SCHEDULED_TASKS = 5;

    private ScheduledExecutorService reportMonitorMetricsExecutor;

    private static volatile MonitorReporter INSTANCE;
    private volatile boolean started = false;

    private static final long REPORT_MONITOR_METRICS_SECONDS = 1;

    private KapConfig kapConfig;
    private Long periodInMilliseconds;

    @VisibleForTesting
    public int reportInitialDelaySeconds = 0;

    private static final int REPORT_QUEUE_CAPACITY = 5000;
    private LinkedBlockingDeque<MonitorMetric> reportQueue = new LinkedBlockingDeque<>(REPORT_QUEUE_CAPACITY);

    private MonitorReporter(KapConfig kapConfig) {
        dataCollectorExecutor = Executors.newScheduledThreadPool(MAX_SCHEDULED_TASKS,
                new NamedThreadFactory("data_collector"));

        reportMonitorMetricsExecutor = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("report_monitor_metrics"));

        this.kapConfig = kapConfig;
        periodInMilliseconds = kapConfig.getMonitorInterval();
    }

    public static MonitorReporter getInstance(KapConfig kapConfig) {
        if (null == INSTANCE) {
            synchronized (MonitorReporter.class) {
                if (null == INSTANCE) {
                    INSTANCE = new MonitorReporter(kapConfig);
                }
            }
        }

        return INSTANCE;
    }

    public static MonitorReporter getInstance() {
        return getInstance(KapConfig.getInstanceFromEnv());
    }

    private static String getLocalIp() {
        String ip = "127.0.0.1";
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("Use the InetAddress get local ip failed!", e);
        }

        return ip;
    }

    private static String getLocalHost() {
        String host = "localhost";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warn("Use the InetAddress get local host failed!", e);
        }
        return host;
    }

    private String getLocalPort() {
        return kapConfig.getKylinConfig().getServerPort();
    }

    private static String getLocalPid() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return runtimeMXBean.getName().split("@")[0];
    }

    private String getNodeType() {
        return kapConfig.getKylinConfig().getServerMode();
    }

    private <T extends MonitorMetric> T createMonitorMetric(T monitorMetric) {
        monitorMetric.setIp(getLocalIp());
        monitorMetric.setHost(getLocalHost());
        monitorMetric.setPort(getLocalPort());
        monitorMetric.setPid(getLocalPid());
        monitorMetric.setNodeType(getNodeType());
        monitorMetric.setCreateTime(System.currentTimeMillis());

        return monitorMetric;
    }

    public QueryMonitorMetric createQueryMonitorMetric() {
        return createMonitorMetric(new QueryMonitorMetric());
    }

    public JobStatusMonitorMetric createJobStatusMonitorMetric() {
        return createMonitorMetric(new JobStatusMonitorMetric());
    }

    public Integer getQueueSize() {
        return reportQueue.size();
    }

    private void initTasks() {
        submit(new AbstractMonitorCollectTask(
                Lists.newArrayList(Constant.SERVER_MODE_ALL, Constant.SERVER_MODE_QUERY)) {
            @Override
            protected MonitorMetric collect() {
                QueryMonitorMetric queryMonitorMetric = createQueryMonitorMetric();

                queryMonitorMetric.setLastResponseTime(SparkContextCanary.getLastResponseTime());
                queryMonitorMetric.setErrorAccumulated(SparkContextCanary.getErrorAccumulated());
                queryMonitorMetric.setSparkRestarting(SparkContextCanary.isSparkRestarting());

                return queryMonitorMetric;
            }
        });
    }

    private void reportMonitorMetrics() {
        try {
            int queueSize = reportQueue.size();
            for (int i = 0; i < queueSize; i++) {
                MonitorMetric monitorMetric = reportQueue.poll(100, TimeUnit.MILLISECONDS);
                if (null == monitorMetric) {
                    logger.warn("Found the MonitorMetric poll from reportQueue is null!");
                    continue;
                }

                MonitorDao.getInstance()
                        .write2InfluxDB(MonitorDao.getInstance().convert2InfluxDBWriteRequest(monitorMetric));
            }
        } catch (Exception e) {
            logger.error("Failed to report monitor metrics to db!", e);
        }
    }

    public void startReporter() {
        reportMonitorMetricsExecutor.scheduleWithFixedDelay(this::reportMonitorMetrics, reportInitialDelaySeconds,
                REPORT_MONITOR_METRICS_SECONDS, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(this::stopReporter));
        started = true;

        initTasks();
        logger.info("MonitorReporter started!");
    }

    @VisibleForTesting
    public void stopReporter() {
        dataCollectorExecutor.shutdownNow();
        reportMonitorMetricsExecutor.shutdownNow();
        started = false;

        logger.info("MonitorReporter stopped!");
    }

    public void submit(AbstractMonitorCollectTask collectTask) {
        if (!kapConfig.isMonitorEnabled()) {
            logger.warn("Monitor reporter is not enabled!");
            return;
        }

        // for UT
        if (!started) {
            logger.warn("MonitorReporter is not started!");
            return;
        }

        if (!collectTask.getRunningServerMode().contains(getNodeType())) {
            logger.info("This node can not run this collect task, serverMode: {}, task serverMode: {}!", getNodeType(),
                    StringUtils.join(collectTask.getRunningServerMode(), ","));
            return;
        }

        dataCollectorExecutor.scheduleWithFixedDelay(collectTask, 0, periodInMilliseconds, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean reportMonitorMetric(MonitorMetric monitorMetric) {
        Preconditions.checkArgument(started, "MonitorReporter is not started!");

        try {
            this.reportQueue.add(monitorMetric);
        } catch (IllegalStateException ie) {
            logger.warn("Monitor metrics report queue is full!", ie);
            return false;
        } catch (Exception e) {
            logger.error("Failed to report MonitorMetric!", e);
            return false;
        }
        return true;
    }
}
