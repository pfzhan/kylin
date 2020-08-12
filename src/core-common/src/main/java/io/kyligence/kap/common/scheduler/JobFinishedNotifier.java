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

package io.kyligence.kap.common.scheduler;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobFinishedNotifier extends SchedulerEventNotifier {
    private long duration;
    private String jobState;
    private Set<String> segmentIds;
    private Set<Long> layoutIds;
    private long dataRangeStart;
    private long dataRangeEnd;

    public JobFinishedNotifier(String project, String subject, long duration, String jobState, Set<String> segmentIds,
            Set<Long> layoutIds, long dataRangeStart, long dataRangeEnd) {
        setProject(project);
        setSubject(subject);
        this.duration = duration;
        this.jobState = jobState;
        this.segmentIds = segmentIds;
        this.layoutIds = layoutIds;
        this.dataRangeStart = dataRangeStart;
        this.dataRangeEnd = dataRangeEnd;
    }

    public JobInfo extractJobInfo() {
        return new JobInfo(project, subject, segmentIds, layoutIds, dataRangeStart, dataRangeEnd, duration, jobState);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class JobInfo {

        @JsonProperty("project")
        private String project;

        @JsonProperty("model_id")
        private String modelId;

        @JsonProperty("segment_ids")
        private Set<String> segmentIds;

        @JsonProperty("index_ids")
        private Set<Long> indexIds;

        @JsonProperty("data_range_start")
        private long dataRangeStart;

        @JsonProperty("data_range_end")
        private long dataRangeEnd;

        @JsonProperty("duration")
        private long duration;

        @JsonProperty("job_state")
        private String state;
    }
}
