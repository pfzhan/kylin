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

package io.kyligence.kap.rest.config.initialize;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.SegmentAutoMergeUtil;
import org.apache.kylin.metadata.model.TimeRange;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetrics;
import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.apache.kylin.rest.util.SpringContext;

@Slf4j
@Component
public class JobSyncListener {
    private static final long RETRY_INTERVAL = 5000L;
    private static final int MAX_RETRY_COUNT = 5;

    private static class SimpleHttpRequestRetryHandler implements HttpRequestRetryHandler {

        @Override
        public boolean retryRequest(IOException exception, int retryTimes, HttpContext httpContext) {
            if (exception == null) {
                return false;
            }
            log.info("Trigger SimpleHttpRequestRetryHandler, exception : " + exception.getClass().getName()
                    + ", retryTimes : " + retryTimes);
            return retryTimes < MAX_RETRY_COUNT;
        }
    }

    private static class SimpleServiceUnavailableRetryStrategy implements ServiceUnavailableRetryStrategy {

        @Override
        public boolean retryRequest(HttpResponse httpResponse, int executionCount, HttpContext httpContext) {
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            log.info("status code: {}, execution count: {}", statusCode, executionCount);
            return executionCount < MAX_RETRY_COUNT && statusCode != HttpStatus.SC_OK;
        }

        @Override
        public long getRetryInterval() {
            return RETRY_INTERVAL;
        }
    }

    // only for test usage
    @Getter
    @Setter
    private boolean jobReadyNotified = false;
    @Getter
    @Setter
    private boolean jobFinishedNotified = false;

    @Subscribe
    public void onJobIsReady(JobReadyNotifier notifier) {
        jobReadyNotified = true;
        NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
    }

    @Subscribe
    public void onJobFinished(JobFinishedNotifier notifier) {
        try {
            NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
            postJobInfo(extractJobInfo(notifier));
        } finally {
            updateMetrics(notifier);
        }
    }

    @Subscribe
    public void onBuildJobFinished(JobFinishedNotifier notifier) {
        try {
            if (notifier.getJobClass().equals(NSparkCubingJob.class.getName()) && notifier.isSucceed()) {
                SegmentAutoMergeUtil.autoMergeSegments(notifier.getProject(), notifier.getSubject(),
                        notifier.getOwner());
            }
        } catch (Throwable e) {
            log.error("Auto merge failed on project {} model {}", notifier.getProject(), notifier.getSubject(), e);
        }
    }

    static JobInfo extractJobInfo(JobFinishedNotifier notifier) {
        Set<String> segmentIds = notifier.getSegmentIds();
        String project = notifier.getProject();
        String dfID = notifier.getSubject();
        NDataflowManager manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow dataflow = manager.getDataflow(dfID);
        List<SegRange> segRangeList = Lists.newArrayList();
        List<SegmentPartitionsInfo> segmentPartitionsInfoList = Lists.newArrayList();
        if (dataflow != null && CollectionUtils.isNotEmpty(segmentIds)) {
            val model = dataflow.getModel();
            val partitionDesc = model.getMultiPartitionDesc();
            for (String id : segmentIds) {
                NDataSegment segment = dataflow.getSegment(id);
                if (segment == null) {
                    continue;
                }
                TimeRange segRange = segment.getTSRange();
                segRangeList.add(new SegRange(id, segRange.getStart(), segRange.getEnd()));
                if (partitionDesc != null && notifier.getSegmentPartitionsMap().get(id) != null
                        && !notifier.getSegmentPartitionsMap().get(id).isEmpty()) {
                    List<SegmentPartitionResponse> SegmentPartitionResponses = segment.getMultiPartitions().stream()
                            .filter(segmentPartition -> notifier.getSegmentPartitionsMap().get(id)
                                    .contains(segmentPartition.getPartitionId()))
                            .map(partition -> {
                                val partitionInfo = partitionDesc.getPartitionInfo(partition.getPartitionId());
                                return new SegmentPartitionResponse(partitionInfo.getId(), partitionInfo.getValues(),
                                        partition.getStatus(), partition.getLastBuildTime(), partition.getSourceCount(),
                                        partition.getStorageSize());
                            }).collect(Collectors.toList());
                    segmentPartitionsInfoList.add(new SegmentPartitionsInfo(id, SegmentPartitionResponses));
                }
            }
        }
        return new JobInfo(notifier.getJobId(), notifier.getProject(), notifier.getSubject(), notifier.getSegmentIds(),
                notifier.getLayoutIds(), notifier.getDuration(), notifier.getJobState(), notifier.getJobType(),
                segRangeList, segmentPartitionsInfoList, notifier.getStartTime(), notifier.getEndTime(),
                notifier.getTag());
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

        @JsonProperty("segment_partition_info")
        private List<SegmentPartitionsInfo> segmentPartitionInfoList;

        @JsonProperty("start_time")
        private long startTime;

        @JsonProperty("end_time")
        private long endTime;

        @JsonProperty("tag")
        private Object tag;

        public JobInfo(String jobId, String project, String subject, Set<String> segmentIds, Set<Long> layoutIds,
                long duration, String jobState, String jobType, List<SegRange> segRanges,
                List<SegmentPartitionsInfo> segmentPartitionInfoList, long startTime, long endTime, Object tag) {
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
            this.segmentPartitionInfoList = segmentPartitionInfoList;
            this.startTime = startTime;
            this.endTime = endTime;
            this.tag = tag;
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

    @Setter
    @Getter
    static class SegmentPartitionsInfo {
        @JsonProperty("segment_id")
        private String segmentId;
        @JsonProperty("partition_info")
        private List<SegmentPartitionResponse> partitionInfo;

        public SegmentPartitionsInfo(String segmentId, List<SegmentPartitionResponse> segmentPartitionResponseList) {
            this.segmentId = segmentId;
            this.partitionInfo = segmentPartitionResponseList;
        }
    }

    static void postJobInfo(JobInfo info) {
        String url = KylinConfig.getInstanceFromEnv().getJobFinishedNotifierUrl();
        log.info("post job info parameter, url : {}, state : {}, segmentId : {}", url, info.getState(),
                info.getSegmentIds());
        if (url == null || info.getSegmentIds() == null || "READY".equalsIgnoreCase(info.getState())) {
            return;
        }

        RequestConfig config = RequestConfig.custom().setSocketTimeout(3000).build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setSSLContext(getTrustAllSSLContext())
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).setDefaultRequestConfig(config)
                .setRetryHandler(new SimpleHttpRequestRetryHandler())
                .setServiceUnavailableRetryStrategy(new SimpleServiceUnavailableRetryStrategy()).build()) {
            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            String username = KylinConfig.getInstanceFromEnv().getJobFinishedNotifierUsername();
            String password = KylinConfig.getInstanceFromEnv().getJobFinishedNotifierPassword();
            if (username != null && password != null) {
                log.info("use basic auth.");
                String basicToken = makeToken(username, password);
                httpPost.addHeader(HttpHeaders.AUTHORIZATION, basicToken);
            }
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

    @SneakyThrows
    public static SSLContext getTrustAllSSLContext() {
        return new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                return true;
            }
        }).build();
    }

    private static String makeToken(String username, String password) {
        String rawTokenString = String.format(Locale.ROOT, "%s:%s", username, password);
        return "Basic " + Base64.getEncoder().encodeToString(rawTokenString.getBytes(Charset.defaultCharset()));
    }

    private void updateMetrics(JobFinishedNotifier notifier) {
        try {
            log.info("Update metrics for {}, duration is {}, waitTime is {}, jobType is {}, state is {}, subject is {}",
                    notifier.getJobId(), notifier.getDuration(), notifier.getWaitTime(), notifier.getJobType(),
                    notifier.getJobState(), notifier.getSubject());
            ExecutableState state = ExecutableState.valueOf(notifier.getJobState());
            String project = notifier.getProject();
            NDataflowManager manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow dataflow = manager.getDataflow(notifier.getSubject());
            recordPrometheusMetric(notifier, SpringContext.getBean(MeterRegistry.class), dataflow==null?"":dataflow.getModelAlias(), state);
            if (state.isFinalState()) {
                long duration = notifier.getDuration();
                MetricsGroup.hostTagCounterInc(MetricsName.JOB_FINISHED, MetricsCategory.PROJECT, project);
                MetricsGroup.hostTagCounterInc(MetricsName.JOB_DURATION, MetricsCategory.PROJECT, project, duration);
                MetricsGroup.hostTagHistogramUpdate(MetricsName.JOB_DURATION_HISTOGRAM, MetricsCategory.PROJECT,
                        project, duration);
                MetricsGroup.hostTagCounterInc(MetricsName.JOB_WAIT_DURATION, MetricsCategory.PROJECT, project,
                        duration);

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
                MetricsGroup.counterInc(MetricsName.JOB_TOTAL_DURATION, MetricsCategory.PROJECT, project, tags,
                        duration);
            }
        } catch (Exception e) {
            log.error("Fail to update metrics.", e);
        }

    }

    public void recordPrometheusMetric(JobFinishedNotifier notifier, MeterRegistry meterRegistry, String modelAlias,
            ExecutableState state) {
        if (!KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
            return;
        }
        if (state.isFinalState() || ExecutableState.ERROR == state) {
            JobTypeEnum jobTypeEnum = JobTypeEnum.getEnumByName(notifier.getJobType());
            DistributionSummary.builder(PrometheusMetrics.JOB_MINUTES.getValue())
                    .tags(MetricsTag.PROJECT.getVal(), notifier.getProject(), MetricsTag.SUCCEED.getVal(),
                            (ExecutableState.SUCCEED == state) + "", MetricsTag.JOB_CATEGORY.getVal(),
                            Objects.isNull(jobTypeEnum) ? "" : jobTypeEnum.getCategory())
                    .distributionStatisticExpiry(Duration.ofDays(1)).register(meterRegistry)
                    .record((notifier.getDuration() + notifier.getWaitTime()) / (60.0 * 1000.0));
            if (StringUtils.isEmpty(modelAlias)) {
                return;
            }

            boolean containPrometheusJobTypeFlag = JobTypeEnum.getJobTypeByCategory(JobTypeEnum.Category.BUILD).stream()
                    .anyMatch(e -> e.toString().equals(notifier.getJobType()));

            if (containPrometheusJobTypeFlag) {
                DistributionSummary.builder(PrometheusMetrics.MODEL_BUILD_DURATION.getValue())
                        .tags(MetricsTag.MODEL.getVal(), modelAlias, MetricsTag.PROJECT.getVal(), notifier.getProject(),
                                MetricsTag.JOB_TYPE.getVal(), notifier.getJobType(), MetricsTag.SUCCEED.getVal(),
                                (ExecutableState.SUCCEED == state) + "")
                        .distributionStatisticExpiry(Duration.ofDays(1)).sla(KylinConfig.getInstanceFromEnv().getMetricsJobSlaMinutes())
                        .register(meterRegistry)
                        .record((notifier.getDuration() + notifier.getWaitTime()) / (60.0 * 1000.0));
            }
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
