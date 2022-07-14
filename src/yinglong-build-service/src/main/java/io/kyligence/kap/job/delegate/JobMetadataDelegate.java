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

package io.kyligence.kap.job.delegate;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.kyligence.kap.job.handler.AbstractJobHandler;
import io.kyligence.kap.job.manager.JobManager;
import io.kyligence.kap.job.service.JobInfoService;
import io.kyligence.kap.rest.delegate.JobMetadataBaseDelegate;
import io.kyligence.kap.rest.delegate.JobMetadataContract;
import io.kyligence.kap.rest.delegate.JobMetadataRequest;
import lombok.val;
import lombok.experimental.Delegate;

@Service
public class JobMetadataDelegate extends BasicService implements JobMetadataContract {

    @Delegate
    @Autowired
    private JobInfoService jobInfoService;

    @Delegate
    private JobMetadataBaseDelegate jobMetadataBaseDelegate = new JobMetadataBaseDelegate();

    @Override
    public String addSecondStorageJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        AbstractJobHandler abstractJobHandler = parseSecondStorageJobHandler(jobMetadataRequest.getSecondStorageJobHandler());
        return jobManager.addJob(jobMetadataRequest.parseJobParam(), abstractJobHandler);
    }

    public AbstractJobHandler parseSecondStorageJobHandler(String handlerEnumName) {
        JobMetadataRequest.SecondStorageJobHandlerEnum secondStorageJobHandlerEnum = JobMetadataRequest.SecondStorageJobHandlerEnum.valueOf(handlerEnumName);
        switch (secondStorageJobHandlerEnum) {
            case SEGMENT_LOAD:
                return new SecondStorageSegmentLoadJobHandler();
            case SEGMENT_CLEAN:
                return new SecondStorageSegmentCleanJobHandler();
            case INDEX_CLEAN:
                return new SecondStorageIndexCleanJobHandler();
            default:
                throw new KylinRuntimeException("Can not create SecondStorageJobHandler.");
        }
    }
}
