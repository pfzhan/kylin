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
package io.kyligence.kap.common.metrics.service;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class JobStatusMonitorMetric extends MonitorMetric {
    public static final String JOB_STATUS_MONITOR_METRIC_TABLE = "tb_job_status";

    @JsonProperty("finished_jobs")
    @Column(name = "finished_jobs")
    private Long finishedJobs = 0L;

    @JsonProperty("pending_jobs")
    @Column(name = "pending_jobs")
    private Long pendingJobs = 0L;

    @JsonProperty("error_jobs")
    @Column(name = "error_jobs")
    private Long errorJobs = 0L;

    @JsonProperty("running_jobs")
    @Column(name = "running_jobs")
    private Long runningJobs = 0L;

    @Override
    public Map<String, Object> getFields() {
        Map<String, Object> fields = super.getFields();
        fields.put("finished_jobs", this.getFinishedJobs());
        fields.put("pending_jobs", this.getPendingJobs());
        fields.put("error_jobs", this.getErrorJobs());
        fields.put("running_jobs", this.getRunningJobs());
        return fields;
    }

    @Override
    public String getTable() {
        return JOB_STATUS_MONITOR_METRIC_TABLE;
    }
}
