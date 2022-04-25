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

package io.kyligence.kap.job;

import javax.annotation.Resource;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.kyligence.kap.job.core.AbstractJobConfig;
import io.kyligence.kap.job.core.config.FileJobConfig;
import io.kyligence.kap.job.core.config.NacosJobConfig;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.mapper.JobScheduleLockMapper;
import io.kyligence.kap.job.scheduler.JobScheduler;

@Component
public class DataLoadingManager implements InitializingBean, DisposableBean {

    private AbstractJobConfig jobConfig;

    private JobScheduler jobScheduler;

    // TODO Share component instance: DataLoadingManager.
    private static DataLoadingManager dataLoadingManager = null;

    @Value("${kylin.config-source}")
    private String configSource;

    @Resource
    private JobInfoMapper jobInfoMapper;

    @Resource
    private JobScheduleLockMapper scheduleLockMapper;

    public static DataLoadingManager getInstance() {
        return dataLoadingManager;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        // TODO Bad design.
        dataLoadingManager = this;

        switch (configSource) {
        case AbstractJobConfig.NACOS_SOURCE:
            jobConfig = new NacosJobConfig();
            break;
        case AbstractJobConfig.FILE_SOURCE:
        default:
            jobConfig = new FileJobConfig();
            break;
        }

        jobScheduler = jobConfig.getJobScheduler();
    }

    @Override
    public void destroy() throws Exception {
        jobScheduler.destroy();
        jobConfig.destroy();
    }

    public AbstractJobConfig getJobConfig() {
        return jobConfig;
    }

    public JobInfoMapper getJobInfoMapper() {
        return jobInfoMapper;
    }

    public JobScheduleLockMapper getScheduleLockMapper() {
        return scheduleLockMapper;
    }
}
