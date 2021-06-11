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
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.streaming.StreamingJobRecord;
import io.kyligence.kap.metadata.streaming.StreamingJobRecordManager;
import io.kyligence.kap.metadata.streaming.StreamingJobStats;
import io.kyligence.kap.metadata.streaming.StreamingJobStatsManager;
import io.kyligence.kap.rest.request.StreamingJobActionEnum;
import io.kyligence.kap.rest.request.StreamingJobFilter;
import io.kyligence.kap.rest.response.StreamingJobDataStatsResponse;
import io.kyligence.kap.rest.response.StreamingJobResponse;
import io.kyligence.kap.streaming.jobs.scheduler.StreamingScheduler;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import lombok.val;
import lombok.var;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

@Component("streamingJobService")
public class StreamingJobService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(StreamingJobService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    public void launchStreamingJob(String project, String modelId, JobTypeEnum jobType) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.submitJob(project, modelId, jobType);
    }

    public void stopStreamingJob(String project, String modelId, JobTypeEnum jobType) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.stopJob(modelId, jobType);
    }

    public void forceStopStreamingJob(String project, String modelId) {
        forceStopStreamingJob(project, modelId, JobTypeEnum.STREAMING_BUILD);
        forceStopStreamingJob(project, modelId, JobTypeEnum.STREAMING_MERGE);
    }

    public void forceStopStreamingJob(String project, String modelId, JobTypeEnum jobType) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, jobType.name()), true);
        try {
            scheduler.killJob(modelId, jobType, JobStatusEnum.STOPPED);
        } finally {
            scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, jobType.name()), false);
        }
    }

    public void updateStreamingJobParams(String project, String jobId, Map<String, String> params) {
        aclEvaluate.checkProjectOperationPermission(project);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(jobId, copyForWrite -> {
                copyForWrite.setParams(params);
            });
            return null;
        }, project);
    }

    public void updateStreamingJobStatus(String project, String modelId, String action) {
        updateStreamingJobStatus(project,
                Arrays.asList(StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name())), action);
        updateStreamingJobStatus(project,
                Arrays.asList(StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name())), action);
    }

    public void updateStreamingJobStatus(String project, List<String> jobIds, String action) {
        aclEvaluate.checkProjectOperationPermission(project);
        StreamingJobActionEnum.validate(action);
        for (int i = 0; i < jobIds.size(); i++) {
            val jobId = jobIds.get(i).split("\\_");
            String modelId = jobId[0];
            String jobType = "STREAMING_" + jobId[1].toUpperCase(Locale.ROOT);
            switch (StreamingJobActionEnum.valueOf(action)) {
            case START:
                launchStreamingJob(project, modelId, JobTypeEnum.valueOf(jobType));
                break;
            case STOP:
                stopStreamingJob(project, modelId, JobTypeEnum.valueOf(jobType));
                break;
            case FORCE_STOP:
                forceStopStreamingJob(project, modelId, JobTypeEnum.valueOf(jobType));
                break;
            case RESTART:
                forceStopStreamingJob(project, modelId, JobTypeEnum.valueOf(jobType));
                launchStreamingJob(project, modelId, JobTypeEnum.valueOf(jobType));
                break;
            default:
                throw new IllegalStateException("This streaming job can not do this action: " + action);
            }
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

    public List<StreamingJobRecord> getStreamingJobRecordList(String project, String jobId) {
        val mgr = StreamingJobRecordManager.getInstance(project);
        return mgr.queryByJobId(jobId);
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

    public void collectStreamingJobStats(StreamingJobStatsRequest statsRequst) {
        val statsMgr = getStreamingJobStatsManager();
        val jobId = statsRequst.getJobId();
        val project = statsRequst.getProject();
        val batchRowNum = statsRequst.getBatchRowNum();
        val rowsPerSecond = statsRequst.getRowsPerSecond();
        val procTime = statsRequst.getProcessingTime();
        val minDataLatency = statsRequst.getMinDataLatency();
        val maxDataLatency = statsRequst.getMaxDataLatency();
        val triggerStartTime = statsRequst.getTriggerStartTime();
        StreamingJobStats stats = new StreamingJobStats(jobId, project, batchRowNum, rowsPerSecond, procTime,
                minDataLatency, maxDataLatency, triggerStartTime);
        try {
            statsMgr.insert(stats);
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

    public DataResult<List<StreamingJobResponse>> getStreamingJobList(StreamingJobFilter jobFilter, int offset,
            int limit) {
        val config = KylinConfig.getInstanceFromEnv();
        List<StreamingJobMeta> list;
        if (StringUtils.isEmpty(jobFilter.getProject())) {
            val prjMgr = NProjectManager.getInstance(config);
            val prjList = prjMgr.listAllProjects();
            list = new ArrayList<>();
            for (ProjectInstance instance : prjList) {
                val mgr = StreamingJobManager.getInstance(config, instance.getName());
                list.addAll(mgr.listAllStreamingJobMeta());
            }
        } else {
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, jobFilter.getProject());
            list = mgr.listAllStreamingJobMeta();
        }
        List<String> jobIdList = list.stream().filter(item -> JobTypeEnum.STREAMING_BUILD == item.getJobType())
                .map(item -> StreamingUtils.getJobId(item.getModelId(), item.getJobType().name()))
                .collect(Collectors.toList());

        val statsMgr = StreamingJobStatsManager.getInstance();
        val dataLatenciesMap = statsMgr.queryDataLatenciesByJobIds(jobIdList);
        List<StreamingJobResponse> respList = list.stream().map(item -> {
            val resp = new StreamingJobResponse(item);
            val jobId = StreamingUtils.getJobId(resp.getModelId(), resp.getJobType().name());
            if (dataLatenciesMap != null && dataLatenciesMap.containsKey(jobId)) {
                resp.setDataLatency(dataLatenciesMap.get(jobId));
            }
            val recordMgr = StreamingJobRecordManager.getInstance(jobFilter.getProject());
            val record = recordMgr.getLatestOneByJobId(jobId);
            if (record != null) {
                resp.setLastStatusDuration(System.currentTimeMillis() - record.getCreateTime());
            }
            return resp;
        }).collect(Collectors.toList());

        Comparator<StreamingJobResponse> comparator = propertyComparator(
                StringUtils.isEmpty(jobFilter.getSortBy()) ? "last_update_time" : jobFilter.getSortBy(),
                !jobFilter.isReverse());
        val filterList = respList.stream().filter(item -> {
            if (StringUtils.isEmpty(jobFilter.getModelName())) {
                return true;
            }
            return item.getModelName().contains(jobFilter.getModelName());
        }).filter(item -> {
            if (CollectionUtils.isEmpty(jobFilter.getModelNames())) {
                return true;
            }
            return jobFilter.getModelNames().contains(item.getModelName());
        }).filter(item -> {
            if (CollectionUtils.isEmpty(jobFilter.getJobTypes())) {
                return true;
            }
            return jobFilter.getJobTypes().contains(item.getJobType().name());
        }).filter(item -> {
            if (CollectionUtils.isEmpty(jobFilter.getStatuses())) {
                return true;
            }
            return jobFilter.getStatuses().contains(item.getCurrentStatus().name());
        }).sorted(comparator).collect(Collectors.toList());
        List<StreamingJobResponse> targetList = PagingUtil.cutPage(filterList, offset, limit).stream()
                .collect(Collectors.toList());
        return new DataResult<>(targetList, targetList.size(), offset, limit);
    }

    public StreamingJobDataStatsResponse getStreamingJobDataStats(String jobId, String project, Integer timeFilter) {
        val config = KylinConfig.getInstanceFromEnv();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        val resp = new StreamingJobDataStatsResponse();
        Message msg = MsgPicker.getMsg();
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        var startTime = System.currentTimeMillis();
        if (timeFilter > 0) {
            switch (timeFilter) {
            case 1:
                calendar.add(Calendar.DAY_OF_MONTH, -1);
                startTime = calendar.getTimeInMillis();
                break;
            case 3:
                calendar.add(Calendar.DAY_OF_MONTH, -3);
                startTime = calendar.getTimeInMillis();
                break;
            case 7:
                calendar.add(Calendar.DAY_OF_MONTH, -7);
                startTime = calendar.getTimeInMillis();
                break;
            default:
                throw new KylinException(INVALID_PARAMETER, msg.getILLEGAL_TIME_FILTER());
            }
            val statsMgr = StreamingJobStatsManager.getInstance();
            val statsList = statsMgr.queryStreamingJobStats(startTime, jobId);
            val consumptionRateList = new ArrayList<Integer>();
            val procTimeList = new ArrayList<Long>();
            val minDataLatencyList = new ArrayList<Long>();
            val createDateList = new ArrayList<Long>();
            statsList.stream().forEach(item -> {
                consumptionRateList.add(item.getRowsPerSecond().intValue());
                procTimeList.add(item.getProcessingTime());
                minDataLatencyList.add(item.getMinDataLatency());
                createDateList.add(item.getCreateTime());
            });
            resp.setConsumptionRateHist(consumptionRateList);
            resp.setProcessingTimeHist(procTimeList);
            resp.setDataLatencyHist(minDataLatencyList);
            resp.setCreateTime(createDateList);
        }
        return resp;
    }

    public StreamingJobResponse getStreamingJobInfo(String jobId, String project) {
        val config = KylinConfig.getInstanceFromEnv();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        StreamingJobMeta jobMeta = mgr.getStreamingJobByUuid(jobId);
        val resp = new StreamingJobResponse(jobMeta);
        val statsMgr = StreamingJobStatsManager.getInstance();
        val stats = statsMgr.getLatestOneByJobId(jobId);
        if (stats != null) {
            resp.setDataLatency(stats.getMinDataLatency());
        }
        val recordMgr = StreamingJobRecordManager.getInstance(project);
        val record = recordMgr.getLatestOneByJobId(jobId);
        if (record != null) {
            resp.setLastStatusDuration(System.currentTimeMillis() - record.getCreateTime());
        }
        return resp;
    }

}
