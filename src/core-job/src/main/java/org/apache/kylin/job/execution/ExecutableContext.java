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

package org.apache.kylin.job.execution;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.KylinConfig;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
@Slf4j
public class ExecutableContext {

    @Getter
    @Setter
    private long epochId;
    @Getter
    @Setter
    private volatile boolean reachQuotaLimit = false;
    @Getter
    @Setter
    private volatile boolean isLicenseOverCapacity = false;

    private final ConcurrentMap<String, Executable> runningJobs;
    private final ConcurrentMap<String, Long> runningJobInfos;
    private final KylinConfig kylinConfig;

    public ExecutableContext(ConcurrentMap<String, Executable> runningJobs,
                             ConcurrentMap<String, Long> runningJobInfos, KylinConfig kylinConfig, long epochId) {
        this.runningJobs = runningJobs;
        this.runningJobInfos = runningJobInfos;
        this.kylinConfig = kylinConfig;
        this.epochId = epochId;
    }

    public KylinConfig getConfig() {
        return kylinConfig;
    }

    public void addRunningJob(Executable executable) {
        runningJobs.put(executable.getId(), executable);
        runningJobInfos.put(executable.getId(), System.currentTimeMillis());
    }

    public void removeRunningJob(Executable executable) {
        runningJobs.remove(executable.getId());
        runningJobInfos.remove(executable.getId());
    }

    public Map<String, Executable> getRunningJobs() {
        return Collections.unmodifiableMap(runningJobs);
    }

    public Map<String, Long> getRunningJobInfos() {
        return Collections.unmodifiableMap(runningJobInfos);
    }
}