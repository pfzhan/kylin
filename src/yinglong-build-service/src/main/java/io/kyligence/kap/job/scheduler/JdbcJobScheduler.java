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

package io.kyligence.kap.job.scheduler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Queues;

import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.engine.spark.utils.ThreadUtils;
import io.kyligence.kap.job.DataLoadingManager;
import io.kyligence.kap.job.core.AbstractJobConfig;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.mapper.JobScheduleLockMapper;

public class JdbcJobScheduler implements JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JdbcJobScheduler.class);

    private final AbstractJobConfig jobConfig;

    private final DataSource dataSource;

    private final JobInfoMapper jobInfoMapper;

    private final JobScheduleLockMapper scheduleLockMapper;

    private final AtomicBoolean isMaster = new AtomicBoolean(false);

    private final String schedulerInstance = AddressUtil.getLocalInstance();

    private static final String JOB_SCHEDULER_MASTER = "job_scheduler_master";

    private static final String SQL_QUERY_1_LOCK = "select 1 from job_schedule_lock where lock_id = ? for update";

    private ScheduledExecutorService standbyScheduler;

    private ScheduledExecutorService masterScheduler;

    private ScheduledExecutorService slaveScheduler;

    private ThreadPoolExecutor consumerPool;

    private BlockingQueue<JobInfo> consumerQueue;

    public JdbcJobScheduler() {
        DataLoadingManager dataLoading = DataLoadingManager.getInstance();
        jobConfig = dataLoading.getJobConfig();
        dataSource = dataLoading.getDataSource();
        jobInfoMapper = dataLoading.getJobInfoMapper();
        scheduleLockMapper = dataLoading.getScheduleLockMapper();
    }

    public void start() {
        standbyScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Standby-Singleton");

        masterScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Master-Singleton");

        slaveScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Slave-Singleton");

        consumerQueue = Queues.newLinkedBlockingQueue();
        consumerPool = ThreadUtils.newDaemonScalableThreadPool("JdbcJobScheduler-Consumer", 1,
                jobConfig.getJobSchedulerConsumerMaxThreads(), 5, TimeUnit.MINUTES);

        scheduleMaster();
        scheduleStandby();
        scheduleProduceJob();
        scheduleConsumeJob();
    }

    @Override
    public void destroy() {
        masterScheduler.shutdownNow();
        standbyScheduler.shutdownNow();
    }

    private Date nextJSMExpireTime() {
        Date current = new Date();
        return DateUtils.addSeconds(current, jobConfig.getJobSchedulerMasterExpireSec());
    }

    private void scheduleStandby() {
        final Runnable acquireMaster = new Runnable() {
            @Override
            public void run() {

                Connection conn = null;
                Boolean autoCommit = null;

                try {
                    conn = dataSource.getConnection();
                    autoCommit = conn.getAutoCommit();
                    conn.setAutoCommit(false);

                    try (PreparedStatement statement = conn.prepareStatement(SQL_QUERY_1_LOCK)) {
                        statement.setString(1, JOB_SCHEDULER_MASTER);
                        try (ResultSet resultSet = statement.executeQuery()) {
                            int r;
                            if (resultSet.next()) {
                                // update
                                r = scheduleLockMapper.updateLock(JOB_SCHEDULER_MASTER, schedulerInstance,
                                        nextJSMExpireTime());
                            } else {
                                // insert
                                r = scheduleLockMapper.insertLock(JOB_SCHEDULER_MASTER, schedulerInstance,
                                        nextJSMExpireTime());
                            }

                            if (r > 0) {
                                // become master
                                logger.info("JdbcJobScheduler acquireMaster success, become master.");
                                isMaster.set(true);
                            } else if (isMaster.get()) {
                                // fallback standby
                                logger.info("JdbcJobScheduler acquireMaster fail, fallback standby.");
                                isMaster.set(false);
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.error("[UNEXPECTED_THINGS_HAPPENED] JobScheduler dataSource#getConnection failed.", e);
                } finally {

                    // conn#commit
                    if (Objects.nonNull(conn)) {
                        try {
                            conn.commit();
                        } catch (SQLException sqle) {
                            logger.error("[UNEXPECTED_THINGS_HAPPENED] JobScheduler acquireMaster conn#commit failed.",
                                    sqle);
                        }

                        // conn#setAutoCommit
                        if (Objects.nonNull(autoCommit)) {
                            try {
                                conn.setAutoCommit(autoCommit);
                            } catch (SQLException sqle) {
                                logger.error(
                                        "[UNEXPECTED_THINGS_HAPPENED] JobScheduler acquireMaster conn#setAutoCommit failed.",
                                        sqle);
                            }
                        }

                        // conn#close
                        try {
                            conn.close();
                        } catch (SQLException sqle) {
                            logger.error("[UNEXPECTED_THINGS_HAPPENED] JobScheduler acquireMaster conn#close failed.",
                                    sqle);
                        }

                    }
                }

            }
        };
        double jsmRenewalRatio = jobConfig.getJobSchedulerMasterRenewalRatio();
        int initialDelay = (int) ((1.0d - jsmRenewalRatio) * jobConfig.getJobSchedulerMasterExpireSec());
        int delay = (int) (jsmRenewalRatio * jobConfig.getJobSchedulerMasterExpireSec());

        standbyScheduler.scheduleWithFixedDelay(acquireMaster, initialDelay, delay, TimeUnit.SECONDS);

    }

    private void scheduleMaster() {
        int delay = jobConfig.getJobSchedulerMasterPollIntervalSec();
        if (isMaster.get()) {
            // 1. only single master exists at concurrent time.
            // 2. master's duty: schedule job.
            masterScheduler.schedule(this::scheduleJob, delay, TimeUnit.SECONDS);
        } else {
            masterScheduler.schedule(this::scheduleMaster, delay, TimeUnit.SECONDS);
        }

    }

    private void scheduleJob() {
        // TODO enum job status
        final String readyStatus = "READY";
        int batchSize = jobConfig.getJobSchedulerMasterPollBatchSize();
        List<JobInfo> readyJobList = jobInfoMapper.selectByStatusBatch(readyStatus, batchSize);
        if (readyJobList.isEmpty()) {
            scheduleMaster();
            return;
        }

        String polledJobIdInfo = readyJobList.stream().map(JobInfo::getJobId)
                .collect(Collectors.joining(",", "[", "]"));
        logger.info("JdbcJobScheduler polled {} jobs: {}", readyJobList.size(), polledJobIdInfo);

        for (JobInfo jobInfo : readyJobList) {
            // TODO insert if not exists
            int r = scheduleLockMapper.insertLock(jobInfo.getJobId(), null, null);
            if (r > 0) {
                // TODO enum job status
                jobInfoMapper.updateJobStatus(jobInfo.getJobId(), "PENDING");
            }

        }

        // maybe un-scheduled job exists, schedule job immediately
        scheduleJob();
    }

    private void scheduleProduceJob() {

    }

    private void scheduleConsumeJob() {

        /*// shuffle jobs avoiding jobLock conflict
        Collections.shuffle(readyJobList);*/

    }

}
