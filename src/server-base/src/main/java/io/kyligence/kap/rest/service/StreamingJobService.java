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

package io.kyligence.kap.rest.service;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.streaming.RowCountDetailByTime;
import io.kyligence.kap.metadata.streaming.StreamingJobStats;
import io.kyligence.kap.metadata.streaming.StreamingJobStatsDAO;
import io.kyligence.kap.metadata.streaming.StreamingStatistics;
import io.kyligence.kap.rest.request.StreamingJobActionEnum;
import io.kyligence.kap.rest.response.StreamingJobResponse;
import io.kyligence.kap.streaming.jobs.scheduler.StreamingScheduler;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

@Component("streamingJobService")
public class StreamingJobService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(StreamingJobService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    public void launchStreamingJob(String project, String modelId) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.submitJob(project, modelId);
    }

    public void stopStreamingJob(String project, String modelId) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.stopJob(modelId);
    }

    public void forceStopStreamingJob(String project, String modelId) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name()), true);
        try {
            scheduler.killJob(modelId, JobTypeEnum.STREAMING_MERGE, JobStatusEnum.STOPPED);
        }finally {
            scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name()), false);
        }

        scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name()), true);
        try{
            scheduler.killJob(modelId, JobTypeEnum.STREAMING_BUILD, JobStatusEnum.STOPPED);
        }finally {
            scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name()), false);
        }
    }

    public void updateStreamingJobParams(String project, String modelId, Map<String, String> buildParams,
            Map<String, String> mergeParams) {
        aclEvaluate.checkProjectOperationPermission(project);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name()),
                    copyForWrite -> {
                        copyForWrite.setParams(buildParams);
                    });
            mgr.updateStreamingJob(StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name()),
                    copyForWrite -> {
                        copyForWrite.setParams(mergeParams);
                    });
            return null;
        }, project);
    }

    public void updateStreamingJobStatus(String project, String modelId, String action) {
        aclEvaluate.checkProjectOperationPermission(project);
        StreamingJobActionEnum.validate(action);
        switch (StreamingJobActionEnum.valueOf(action)) {
        case START:
            launchStreamingJob(project, modelId);
            break;
        case STOP:
            stopStreamingJob(project, modelId);
            break;
        case FORCE_STOP:
            forceStopStreamingJob(project, modelId);
            break;
        default:
            throw new IllegalStateException("This streaming job can not do this action: " + action);
        }
    }

    public void updateStreamingJobInfo(StreamingJobUpdateRequest streamingJobUpdateRequest) {
        String project = streamingJobUpdateRequest.getProject();
        String modelId = streamingJobUpdateRequest.getModelId();
        String jobType = streamingJobUpdateRequest.getJobType();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(StreamingUtils.getJobId(modelId, jobType), copyForWrite -> {
                copyForWrite.setProcessId(streamingJobUpdateRequest.getProcessId());
                copyForWrite.setNodeInfo(streamingJobUpdateRequest.getNodeInfo());
                copyForWrite.setYarnAppId(streamingJobUpdateRequest.getYarnAppId());
                copyForWrite.setYarnAppUrl(streamingJobUpdateRequest.getYarnAppUrl());
                SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                        Locale.getDefault(Locale.Category.FORMAT));
                copyForWrite.setLastUpdateTime(simpleFormat.format(new Date()));
            });
            return null;
        }, project);
    }

    public String addSegment(String project, String modelId, SegmentRange rangeToMerge, String currLayer,
            String newSegId) {
        if (!StringUtils.isEmpty(currLayer)) {
            int layer = Integer.parseInt(currLayer) + 1;
            NDataSegment afterMergeSeg = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                val df = dfMgr.getDataflow(modelId);
                if (df != null) {
                    return dfMgr.mergeSegments(df, rangeToMerge, true, layer, newSegId);
                } else {
                    return null;
                }
            }, project);
            return getSegmentId(afterMergeSeg);
        } else {
            val newSegment = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                NDataflow df = dfMgr.getDataflow(modelId);
                if (df != null) {
                    return dfMgr.appendSegmentForStreaming(df, rangeToMerge, newSegId);
                } else {
                    return null;
                }
            }, project);
            return getSegmentId(newSegment);
        }
    }

    private String getSegmentId(NDataSegment newSegment) {
        return newSegment != null ? newSegment.getId() : StringUtils.EMPTY;
    }

    public void updateSegment(String project, String modelId, String segId, List<NDataSegment> removeSegmentList,
            String status) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val df = dfMgr.getDataflow(modelId);
            if (df != null) {
                NDataflow copy = df.copy();
                val seg = copy.getSegment(segId);
                seg.setStatus(SegmentStatusEnum.READY);
                val dfUpdate = new NDataflowUpdate(modelId);
                dfUpdate.setToUpdateSegs(seg);
                if (removeSegmentList != null) {
                    dfUpdate.setToRemoveSegs(removeSegmentList.toArray(new NDataSegment[0]));
                }
                if (!StringUtils.isEmpty(status)) {
                    dfUpdate.setStatus(RealizationStatusEnum.valueOf(status));
                }
                dfMgr.updateDataflow(dfUpdate);
            }
            return 0;
        }, project);
    }

    public void deleteSegment(String project, String modelId, List<NDataSegment> removeSegmentList) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

            val dfUpdate = new NDataflowUpdate(modelId);
            if (removeSegmentList != null) {
                dfUpdate.setToRemoveSegs(removeSegmentList.toArray(new NDataSegment[0]));
            }
            if (dfMgr.getDataflow(modelId) != null) {
                dfMgr.updateDataflow(dfUpdate);
            }
            return 0;
        }, project);
    }

    public void updateLayout(String project, String modelId, List<NDataLayout> layouts) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
            val mgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            if (mgr.getDataflow(modelId) != null) {
                mgr.updateDataflow(update);
            }
            return 0;
        }, project);
    }

    public void collectStreamingJobStats(String jobId, String project, Long batchRowNum, Double rowsPerSecond,
            Long durationMs, Long triggerStartTime) {
        StreamingJobStatsDAO sjsDao = getStreamingJobStatsDao();
        StreamingJobStats stats = new StreamingJobStats(jobId, project, batchRowNum, rowsPerSecond, durationMs,
                triggerStartTime);
        try {
            sjsDao.insert(stats);
        } catch (Exception exception) {
            logger.error("Write streaming job stats failed...");
        }
        Date date = new Date(System.currentTimeMillis());
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                    Locale.getDefault(Locale.Category.FORMAT));
            format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(jobId, copyForWrite -> {
                copyForWrite.setLastUpdateTime(format.format(date));
                copyForWrite.setLastBatchCount(batchRowNum.intValue());
            });
            return null;
        }, project);
    }

    public StreamingJobResponse getStreamingJobInfo(String modelId, String project) {
        String jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        String mergeId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());

        val config = KylinConfig.getInstanceFromEnv();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        StreamingJobMeta meta = mgr.getStreamingJobByUuid(jobId);
        StreamingJobMeta mergeMeta = mgr.getStreamingJobByUuid(mergeId);
        if (JobStatusEnum.RUNNING != meta.getCurrentStatus()) {
            return new StreamingJobResponse(meta, mergeMeta);
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        Long currentTotalCount = 0L;
        Long currentDuation = 0L;
        Long lastUpdateTime = 0L;
        Long lastStartTime = 0L;
        Long lastCreateTime = 0L;
        Long latency = 0L;
        Long currentTime = System.currentTimeMillis();
        StreamingJobStatsDAO sjsDao = getStreamingJobStatsDao();

        try {
            if (meta.getLastStartTime() != null) {
                lastStartTime = format.parse(meta.getLastStartTime()).getTime();
            }
            if (meta.getLastUpdateTime() != null) {
                lastUpdateTime = format.parse(meta.getLastUpdateTime()).getTime();
            }
            StreamingStatistics statistics = sjsDao.getStreamingStatistics(lastStartTime, jobId);
            if (statistics != null && statistics.getCount() > 0) {
                currentTotalCount = statistics.getCount();
            }
            currentDuation = currentTime - lastStartTime;
        } catch (ParseException exception) {
            logger.warn("Time format could not be parsed...");
        }
        Long countsIn5mins = 0L;
        Long countsIn15mins = 0L;
        Long countsIn30mins = 0L;
        List<RowCountDetailByTime> counts = sjsDao.queryRowCountDetailByTime(lastStartTime, jobId);
        if (counts.size() > 0) {
            for (RowCountDetailByTime rowCount : counts) {
                if (rowCount.getCreateTime() > currentTime - 5 * 60 * 1000) {
                    countsIn5mins += rowCount.getBatchRowNum();
                    countsIn15mins += rowCount.getBatchRowNum();
                    countsIn30mins += rowCount.getBatchRowNum();
                } else if (rowCount.getCreateTime() > currentTime - 15 * 60 * 1000) {
                    countsIn15mins += rowCount.getBatchRowNum();
                    countsIn30mins += rowCount.getBatchRowNum();
                } else if (rowCount.getCreateTime() >= currentTime - 30 * 60 * 1000) {
                    countsIn30mins += rowCount.getBatchRowNum();
                }
            }
            lastCreateTime = counts.get(0).getCreateTime();
            latency = lastUpdateTime - lastCreateTime;
        }
        return new StreamingJobResponse(meta, mergeMeta, lastCreateTime, latency, lastUpdateTime,
                (countsIn5mins / (double) (5 * 60)), (countsIn15mins / (double) (15 * 60)),
                (countsIn30mins / (double) (30 * 60)), (currentTotalCount * (double) 1000 / currentDuation));
    }
}
