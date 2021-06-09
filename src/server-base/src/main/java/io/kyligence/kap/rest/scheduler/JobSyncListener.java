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

package io.kyligence.kap.rest.scheduler;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.TimeRange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class JobSyncListener {

    @Getter
    @Setter
    private boolean jobFinishedNotified = false;

    @Subscribe
    public void onJobFinished(JobFinishedNotifier notifier) {
        try {
            jobFinishedNotified = true;
            NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
            postJobInfo(extractJobInfo(notifier));
        } finally {
            updateMetrics(notifier);
        }
    }


    static JobInfo extractJobInfo(JobFinishedNotifier notifier) {
        Set<String> segmentIds = notifier.getSegmentIds();
        String project = notifier.getProject();
        String dfID = notifier.getSubject();
        NDataflowManager manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow dataflow = manager.getDataflow(dfID);
        List<SegRange> list = Lists.newArrayList();
        if (dataflow != null) {
            for (String id : segmentIds) {
                TimeRange segRange = dataflow.getSegment(id).getTSRange();
                list.add(new SegRange(id, segRange.getStart(), segRange.getEnd()));
            }
        }
        return new JobInfo(notifier.getJobId(), notifier.getProject(), notifier.getSubject(), notifier.getSegmentIds(),
                notifier.getLayoutIds(), notifier.getDuration(), notifier.getJobState(), notifier.getJobType(), list);
    }

    @Getter
    @Setter
    public static class JobInfo {

        @JsonProperty("job_id")
        private String jobId;

        @JsonProperty("project")
        private String project;

        @JsonProperty("model_id")
        private String modelId;

        @JsonProperty("segment_ids")
        private Set<String> segmentIds;

        @JsonProperty("index_ids")
        private Set<Long> indexIds;

        @JsonProperty("duration")
        private long duration;

        @JsonProperty("job_state")
        private String state;

        @JsonProperty("job_type")
        private String jobType;

        @JsonProperty("segment_time_range")
        private List<SegRange> segRanges;

        public JobInfo(String jobId, String project, String subject, Set<String> segmentIds, Set<Long> layoutIds,
                       long duration, String jobState, String jobType, List<SegRange> segRanges) {
            this.jobId = jobId;
            this.project = project;
            this.modelId = subject;
            this.segmentIds = segmentIds;
            this.indexIds = layoutIds;
            this.duration = duration;
            if ("SUICIDAL".equalsIgnoreCase(jobState)) {
                this.state = "DISCARDED";
            } else {
                this.state = jobState;
            }
            this.jobType = jobType;
            this.segRanges = segRanges;
        }
    }

    @Setter
    @Getter
    static class SegRange {
        @JsonProperty("segment_id")
        private String segmentId;

        @JsonProperty("data_range_start")
        private long start;

        @JsonProperty("data_range_end")
        private long end;

        public SegRange(String id, long start, long end) {
            this.segmentId = id;
            this.start = start;
            this.end = end;
        }
    }

    static void postJobInfo(JobInfo info) {
        String url = KylinConfig.getInstanceFromEnv().getJobFinishedNotifierUrl();

        if (url == null || info.getSegmentIds() == null || "READY".equalsIgnoreCase(info.getState())) {
            return;
        }

        RequestConfig config = RequestConfig.custom().setSocketTimeout(3000).build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build()) {
            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            httpPost.setEntity(new StringEntity(JsonUtil.writeValueAsString(info), StandardCharsets.UTF_8));
            HttpResponse response = httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                log.info("Post job info to " + url + " successful.");
            } else {
                log.info("Post job info to " + url + " failed. Status code: " + code);
            }
        } catch (IOException e) {
            log.warn("Error occurred when post job status.", e);
        }
    }

    private void updateMetrics(JobFinishedNotifier notifier) {
        try {
            log.info("Update metrics for {}, duration is {}, waitTime is {}, jobType is {}, state is {}, subject is {}",
                    notifier.getJobId(), notifier.getDuration(), notifier.getWaitTime(), notifier.getJobType(), notifier.getJobState(),
                    notifier.getSubject());
            ExecutableState state = ExecutableState.valueOf(notifier.getJobState());
            String project = notifier.getProject();
            if (state.isFinalState()) {
                long duration = notifier.getDuration();
                MetricsGroup.hostTagCounterInc(MetricsName.JOB_FINISHED, MetricsCategory.PROJECT, project);
                MetricsGroup.hostTagCounterInc(MetricsName.JOB_DURATION, MetricsCategory.PROJECT, project, duration);
                MetricsGroup.hostTagHistogramUpdate(MetricsName.JOB_DURATION_HISTOGRAM, MetricsCategory.PROJECT,
                        project, duration);
                MetricsGroup.hostTagCounterInc(MetricsName.JOB_WAIT_DURATION, MetricsCategory.PROJECT, project, duration);

                NDataflowManager manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                NDataflow dataflow = manager.getDataflow(notifier.getSubject());
                if (dataflow != null) {
                    String modelAlias = dataflow.getModelAlias();
                    Map<String, String> tags = Maps.newHashMap();
                    tags.put(MetricsTag.MODEL.getVal(), project.concat("-").concat(modelAlias));
                    MetricsGroup.counterInc(MetricsName.MODEL_BUILD_DURATION, MetricsCategory.PROJECT, project, tags,
                            duration);
                    MetricsGroup.counterInc(MetricsName.MODEL_WAIT_DURATION, MetricsCategory.PROJECT, project, tags,
                            notifier.getWaitTime());
                    MetricsGroup.histogramUpdate(MetricsName.MODEL_BUILD_DURATION_HISTOGRAM, MetricsCategory.PROJECT,
                            project, tags, duration);
                }

                Map<String, String> tags = getJobStatisticsTags(notifier.getJobType());
                if (state == ExecutableState.SUCCEED) {
                    MetricsGroup.counterInc(MetricsName.SUCCESSFUL_JOB_COUNT, MetricsCategory.PROJECT, project, tags);
                } else if (ExecutableState.ERROR == state) {
                    MetricsGroup.hostTagCounterInc(MetricsName.JOB_ERROR, MetricsCategory.PROJECT, project);
                    MetricsGroup.counterInc(MetricsName.ERROR_JOB_COUNT, MetricsCategory.PROJECT, project, tags);
                }

                if (duration <= 5 * 60 * 1000) {
                    MetricsGroup.counterInc(MetricsName.JOB_COUNT_LT_5, MetricsCategory.PROJECT, project, tags);
                } else if (duration <= 10 * 60 * 1000) {
                    MetricsGroup.counterInc(MetricsName.JOB_COUNT_5_10, MetricsCategory.PROJECT, project, tags);
                } else if (duration <= 30 * 60 * 1000) {
                    MetricsGroup.counterInc(MetricsName.JOB_COUNT_10_30, MetricsCategory.PROJECT, project, tags);
                } else if (duration <= 60 * 60 * 1000) {
                    MetricsGroup.counterInc(MetricsName.JOB_COUNT_30_60, MetricsCategory.PROJECT, project, tags);
                } else {
                    MetricsGroup.counterInc(MetricsName.JOB_COUNT_GT_60, MetricsCategory.PROJECT, project, tags);
                }
                MetricsGroup.counterInc(MetricsName.JOB_TOTAL_DURATION, MetricsCategory.PROJECT, project, tags, duration);
            }
        } catch (Exception e) {
            log.error("Fail to update metrics.", e);
        }

    }

    /**
     * Tags of job statistics used in prometheus
     */
    private Map<String, String> getJobStatisticsTags(String jobType) {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), AddressUtil.getZkLocalInstance());
        tags.put(MetricsTag.JOB_TYPE.getVal(), jobType);
        return tags;
    }
}
