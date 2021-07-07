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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobFinishedNotifier extends SchedulerEventNotifier {
    private String jobId;
    private long duration;
    private String jobState;
    private String jobType;
    private Set<String> segmentIds;
    private Set<Long> layoutIds;
    private long waitTime;
    private Map<String, Set<Long>> segmentPartitionsMap;
    private String jobClass;
    private String owner;
    private boolean isSucceed;

    public JobFinishedNotifier(String jobId, String project, String subject, long duration, String jobState,
            String jobType, Set<String> segmentIds, Set<Long> layoutIds, long waitTime, String jobClass) {
        this(jobId, project, subject, duration, jobState, jobType, segmentIds, layoutIds, waitTime, jobClass, "", true,
                Sets.newHashSet());
    }

    public JobFinishedNotifier(String jobId, String project, String subject, long duration, String jobState,
            String jobType, Set<String> segmentIds, Set<Long> layoutIds, long waitTime, String jobClass, String owner,
            boolean result, Set<Long> partitionIds) {
        setProject(project);
        setSubject(subject);
        this.jobId = jobId;
        this.duration = duration;
        this.jobState = jobState;
        this.jobType = jobType;
        this.segmentIds = segmentIds;
        this.layoutIds = layoutIds;
        this.waitTime = waitTime;
        if (partitionIds != null) {
            this.segmentPartitionsMap = new HashMap<>();
            for (String segmentId : segmentIds) {
                segmentPartitionsMap.put(segmentId, partitionIds);
            }
        }
        this.jobClass = jobClass;
        this.owner = owner;
        this.isSucceed = result;
    }
}