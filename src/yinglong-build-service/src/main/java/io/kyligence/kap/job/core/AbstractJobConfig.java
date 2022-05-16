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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.job.scheduler.JdbcJobScheduler;
import io.kyligence.kap.job.scheduler.JobScheduler;
import io.kyligence.kap.job.scheduler.ProgressReporter;
import io.kyligence.kap.job.scheduler.RestfulProgressReporter;

public abstract class AbstractJobConfig {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJobConfig.class);

    private final ConcurrentMap<String, Object> singletonMap = Maps.newConcurrentMap();

    public static final String FILE_SOURCE = "file";

    public static final String NACOS_SOURCE = "nacos";

    private static final String JOB_SCHEDULER_CLASS_NAME = "jobSchedulerClassName";

    private static final String JOB_PROGRESS_REPORTER_CLASS_NAME = "jobProgressReporterClassName";

    public abstract String getProperty(String name);

    public abstract void destroy();

    public double getMaxLocalNodeMemoryRatio() {
        return 0.5d;
    }

    public int getJobSchedulerMasterPollBatchSize() {
        return 10;
    }

    public int getJobSchedulerMasterPollIntervalSec() {
        return 30;
    }

    public double getJobSchedulerMasterRenewalRatio() {
        return 0.85d;
    }

    public int getJobSchedulerMasterExpireSec() {
        return 60;
    }

    public int getJobSchedulerProducerPollBatchSize() {
        return 5;
    }

    public int getJobSchedulerProducerPollIntervalSec() {
        return 20;
    }

    public double getJobSchedulerConsumerRenewalRatio() {
        return 0.75d;
    }

    public int getJobSchedulerConsumerExpireSec() {
        return 120;
    }

    public int getJobSchedulerConsumerMaxThreads() {
        return 8;
    }

    public int getJobProgressReporterMaxThreads() {
        return 6;
    }

    public JobScheduler getJobScheduler() {
        String prop = null;
        try {
            prop = getProperty(JOB_SCHEDULER_CLASS_NAME);
            return getInstance0(prop);
        } catch (Exception e) {
            logger.error("Create instance from '{}' failed, fallback to default#JdbcJobScheduler.", prop, e);
            return new JdbcJobScheduler();
        }
    }

    public ProgressReporter getJobProgressReporter() {
        String prop = null;
        try {
            prop = getProperty(JOB_PROGRESS_REPORTER_CLASS_NAME);
            return getInstance0(prop);
        } catch (Exception e) {
            logger.error("Create instance from '{}' failed, fallback to default#RestfulProgressReporter.", prop, e);
            return new RestfulProgressReporter();
        }
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
