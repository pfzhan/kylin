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

package org.apache.kylin.job.delegate;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.service.JobInfoService;
import org.apache.kylin.rest.delegate.JobMetadataBaseDelegate;
import org.apache.kylin.rest.delegate.JobMetadataContract;
import org.apache.kylin.rest.delegate.JobMetadataRequest;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
