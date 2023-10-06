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

package org.apache.kylin.job.handler;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.model.JobParam;

public abstract class AbstractSecondStorageJobHanlder extends AbstractJobHandler {

    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        String project = jobParam.getProject();
        Preconditions.checkNotNull(project);
    }

    @Override
    protected boolean needComputeJobBucket() {
        return false;
    }
}
