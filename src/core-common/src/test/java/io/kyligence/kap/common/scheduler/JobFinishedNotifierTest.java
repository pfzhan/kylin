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

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class JobFinishedNotifierTest {
    @Test
    public void testConstructFunction() {
        String jobId = "test_job_id";
        String project = "test_project";
        String subject = "test_model";
        long duration = 1000L;
        long waitTime = 0L;
        String jobState = "test_job_state";
        String jobType = "test_job_type";
        Set<String> segIds = new HashSet<>();
        segIds.add("test_segment_1");
        segIds.add("test_segment_2");
        Set<Long> layoutIds = new HashSet<>();
        layoutIds.add(1L);
        layoutIds.add(2L);
        Set<Long> partitionIds = new HashSet<>();

        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, waitTime, partitionIds);
        Assert.assertEquals(jobId, notifier.getJobId());
        Assert.assertEquals(project, notifier.getProject());
        Assert.assertEquals(subject, notifier.getSubject());
        Assert.assertEquals(duration, notifier.getDuration());
        Assert.assertEquals(jobState, notifier.getJobState());
        Assert.assertEquals(jobType, notifier.getJobType());
        Assert.assertEquals(segIds, notifier.getSegmentIds());
        Assert.assertEquals(layoutIds, notifier.getLayoutIds());
    }
}
