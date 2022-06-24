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
package org.apache.kylin.job.impl.threadpool;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableState;

import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

public class ExecutableDurationContext {
    @Getter
    private String project;
    @Getter
    private String jobId;
    @Getter
    private List<Record> stepRecords;
    @Getter
    private Record record;

    public ExecutableDurationContext(String project, String jobId) {
        this.project = project;
        this.jobId = jobId;
        val manager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        DefaultChainedExecutable job = (DefaultChainedExecutable) manager.getJob(jobId);
        record = new Record(job.getStatus(), job.getDuration(), job.getWaitTime(), job.getCreateTime());
        val steps = job.getTasks();
        stepRecords = steps.stream()
                .map(step -> new Record(step.getStatus(), step.getDuration(), step.getWaitTime(), step.getCreateTime()))
                .collect(Collectors.toList());
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Record {
        private ExecutableState state;
        private long duration;
        private long waitTime;
        private long createTime;
    }
}
