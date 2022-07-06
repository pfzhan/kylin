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

package io.kyligence.kap.job.core;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

public abstract class AbstractJobConfig {

    private long jobSchedulerMasterPollIntervalSec = 30L;

    private long jobSchedulerSlavePollIntervalSec = 20L;

    private static final Logger logger = LoggerFactory.getLogger(AbstractJobConfig.class);

    private final ConcurrentMap<String, Object> singletonMap = Maps.newConcurrentMap();

    public static final String FILE_SOURCE = "file";

    public static final String NACOS_SOURCE = "nacos";

    public abstract String getProperty(String name);

    public abstract void destroy();

    public int getParallelJobCountThreshold() {
        return 20;
    }

    public int getNodeParallelJobCountThreshold() {
        return 5;
    }

    public double getMaxLocalNodeMemoryRatio() {
        return 0.5d;
    }

    public boolean getAutoSetConcurrentJob() {
        return KylinConfig.getInstanceFromEnv().getAutoSetConcurrentJob();
    }

    public int getJobSchedulerMasterPollBatchSize() {
        return 10;
    }

    public long getJobSchedulerMasterPollIntervalSec() {
        return jobSchedulerMasterPollIntervalSec;
    }

    // for ut only
    @VisibleForTesting
    public void setJobSchedulerMasterPollIntervalSec(long val) {
        jobSchedulerMasterPollIntervalSec = val;
    }

    public double getJobSchedulerMasterRenewalRatio() {
        return 0.85d;
    }

    public long getJobSchedulerMasterRenewalSec() {
        return 60L;
    }

    public int getJobSchedulerSlavePollBatchSize() {
        return 5;
    }

    public long getJobSchedulerSlavePollIntervalSec() {
        return jobSchedulerSlavePollIntervalSec;
    }

    // for ut only
    @VisibleForTesting
    public void setJobSchedulerSlavePollIntervalSec(long val) {
        jobSchedulerSlavePollIntervalSec = val;
    }

    public double getJobSchedulerJobRenewalRatio() {
        return 0.75d;
    }

    public long getJobSchedulerJobRenewalSec() {
        return 120L;
    }

    public int getJobSchedulerConsumerMaxThreads() {
        return 8;
    }

    public int getJdbcJobLockClientMaxThreads() {
        return 8;
    }

    public int getJobProgressReporterMaxThreads() {
        return 6;
    }

    // TODO implement this method instead of calling from kylinConfig
    public String getJobTmpOutputStorePath(String project, String jobId) {
        return KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId);
    }

    public CliCommandExecutor getCliCommandExecutor() {
        return KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
    }

    private <T> T getInstance0(String className) throws ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        Object instance = singletonMap.get(className);
        if (Objects.isNull(instance)) {
            synchronized (this) {
                instance = singletonMap.get(className);
                if (Objects.isNull(instance)) {
                    instance = Class.forName(className).getConstructor().newInstance();
                    singletonMap.put(className, instance);
                }
            }
        }
        return (T) instance;
    }
}
