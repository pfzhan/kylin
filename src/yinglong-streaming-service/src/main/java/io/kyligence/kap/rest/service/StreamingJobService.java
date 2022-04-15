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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
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
import io.kyligence.kap.rest.util.ModelUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.jobs.scheduler.StreamingScheduler;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.val;

@Component("streamingJobService")
public class StreamingJobService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(StreamingJobService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("indexPlanService")
    IndexPlanService indexPlanService;

    public void launchStreamingJob(String project, String modelId, JobTypeEnum jobType) {
        checkModelStatus(project, modelId, jobType);
        ModelUtils.checkPartitionColumn(project, modelId, MsgPicker.getMsg().getPARTITION_COLUMN_START_ERROR());
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.submitJob(project, modelId, jobType);
    }

    public void checkModelStatus(String project, String modelId, JobTypeEnum jobType) {
        String jobId = StreamingUtils.getJobId(modelId, jobType.name());
        val config = KylinConfig.getInstanceFromEnv();
        val jobMeta = getManager(StreamingJobManager.class, project).getStreamingJobByUuid(jobId);

        val modelMgr = NDataModelManager.getInstance(config, jobMeta.getProject());
        val model = modelMgr.getDataModelDesc(jobMeta.getModelId());
        if (model.isBroken() || isBatchModelBroken(model)) {
            throw new KylinException(ServerErrorCode.JOB_START_FAILURE, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getJOB_BROKEN_MODEL_START_FAILURE(), model.getAlias()));
        }
    }

    public void stopStreamingJob(String project, String modelId, JobTypeEnum jobType) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.stopJob(modelId, jobType);
    }

    public void forceStopStreamingJob(String project, String modelId, JobTypeEnum jobType) {
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, jobType.name()), true);

        try {
            MetaInfoUpdater.updateJobState(project, StreamingUtils.getJobId(modelId, jobType.name()),
                    JobStatusEnum.STOPPING);
            scheduler.killJob(modelId, jobType, JobStatusEnum.STOPPED);
        } finally {
            scheduler.skipJobListener(project, StreamingUtils.getJobId(modelId, jobType.name()), false);
        }
    }

    public void updateStreamingJobParams(String project, String jobId, Map<String, String> params) {
        aclEvaluate.checkProjectOperationPermission(project);
        checkJobParams(params);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(jobId, copyForWrite -> copyForWrite.setParams(params));
            return null;
        }, project);
    }

    private void checkJobParams(Map<String, String> params) {
        val tableRefreshInterval = params.get(StreamingConstants.STREAMING_TABLE_REFRESH_INTERVAL);
        try {
            StreamingUtils.parseTableRefreshInterval(tableRefreshInterval);
        } catch (Exception e) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_CUSTOMIZE_FORMAT());
        }
    }

    public void updateStreamingJobStatus(String project, List<String> jobIds, String action) {
        StreamingJobActionEnum.validate(action);
        String jobProject = project;
        Map<String, StreamingJobMeta> jobMap = getAllStreamingJobs(project).stream()
                .collect(Collectors.toMap(StreamingJobMeta::getUuid, meta -> meta));
        for (String jobId : jobIds) {
            if (jobMap.containsKey(jobId)) {
                jobProject = jobMap.get(jobId).getProject();
            }
            aclEvaluate.checkProjectOperationPermission(jobProject);
            val jobIdPair = jobId.split("\\_");
            String modelId = jobIdPair[0];
            String jobType = "STREAMING_" + jobIdPair[1].toUpperCase(Locale.ROOT);

            switch (StreamingJobActionEnum.valueOf(action)) {
            case START:
                launchStreamingJob(jobProject, modelId, JobTypeEnum.valueOf(jobType));
                break;
            case STOP:
                stopStreamingJob(jobProject, modelId, JobTypeEnum.valueOf(jobType));
                break;
            case FORCE_STOP:
                forceStopStreamingJob(jobProject, modelId, JobTypeEnum.valueOf(jobType));
                break;
            case RESTART:
                forceStopStreamingJob(jobProject, modelId, JobTypeEnum.valueOf(jobType));
                launchStreamingJob(jobProject, modelId, JobTypeEnum.valueOf(jobType));
                break;
            default:
                break;
            }
        }
    }

    public StreamingJobMeta updateStreamingJobInfo(StreamingJobUpdateRequest streamingJobUpdateRequest) {
        String project = streamingJobUpdateRequest.getProject();
        String modelId = streamingJobUpdateRequest.getModelId();
        String jobType = streamingJobUpdateRequest.getJobType();
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            return mgr.updateStreamingJob(StreamingUtils.getJobId(modelId, jobType), copyForWrite -> {
                copyForWrite.setProcessId(streamingJobUpdateRequest.getProcessId());
                copyForWrite.setNodeInfo(streamingJobUpdateRequest.getNodeInfo());
                copyForWrite.setYarnAppId(streamingJobUpdateRequest.getYarnAppId());
                copyForWrite.setYarnAppUrl(streamingJobUpdateRequest.getYarnAppUrl());
                SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                        Locale.getDefault(Locale.Category.FORMAT));
                copyForWrite.setLastUpdateTime(simpleFormat.format(new Date()));
                val execId = copyForWrite.getJobExecutionId();
                copyForWrite.setJobExecutionId(execId == null ? 1 : (execId + 1));
            });
        }, project);
    }

    public List<StreamingJobRecord> getStreamingJobRecordList(String jobId) {
        val mgr = StreamingJobRecordManager.getInstance();
        return mgr.queryByJobId(jobId);
    }

    public String addSegment(String project, String modelId, SegmentRange<?> rangeToMerge, String currLayer,
            String newSegId) {
        if (!StringUtils.isEmpty(currLayer)) {
            int layer = Integer.parseInt(currLayer) + 1;
            NDataSegment afterMergeSeg = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                val df = dfMgr.getDataflow(modelId);
                return df != null ? dfMgr.mergeSegments(df, rangeToMerge, true, layer, newSegId) : null;
            }, project);
            return getSegmentId(afterMergeSeg);
        } else {
            val newSegment = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                NDataflow df = dfMgr.getDataflow(modelId);
                return df != null ? dfMgr.appendSegmentForStreaming(df, rangeToMerge, newSegId) : null;
            }, project);
            return getSegmentId(newSegment);
        }
    }

    private String getSegmentId(NDataSegment newSegment) {
        return newSegment != null ? newSegment.getId() : StringUtils.EMPTY;
    }

    public void updateSegment(String project, String modelId, String segId, List<NDataSegment> removeSegmentList,
            String status, Long sourceCount) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            Long defaultSourceCount = -1L;
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val df = dfMgr.getDataflow(modelId);
            if (df != null) {
                NDataflow copy = df.copy();
                val seg = copy.getSegment(segId);
                if (!defaultSourceCount.equals(sourceCount)) {
                    seg.setSourceCount(sourceCount);
                }
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

    public StreamingJobStatsManager getStreamingJobStatsManager() {
        return StreamingJobStatsManager.getInstance();
    }

    public DataResult<List<StreamingJobResponse>> getStreamingJobList(StreamingJobFilter jobFilter, int offset,
            int limit) {
        if (!StringUtils.isEmpty(jobFilter.getProject())) {
            aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        }
        List<StreamingJobMeta> list = CollectionUtils.isNotEmpty(jobFilter.getJobIds())
                ? getAllStreamingJobsById(jobFilter.getProject(), jobFilter.getJobIds())
                : getAllStreamingJobs(jobFilter.getProject());

        List<String> jobIdList = list.stream().filter(item -> JobTypeEnum.STREAMING_BUILD == item.getJobType())
                .map(item -> StreamingUtils.getJobId(item.getModelId(), item.getJobType().name()))
                .collect(Collectors.toList());

        val statsMgr = StreamingJobStatsManager.getInstance();
        val dataLatenciesMap = statsMgr.queryDataLatenciesByJobIds(jobIdList);
        List<StreamingJobResponse> respList = list.stream().map(item -> {
            val resp = new StreamingJobResponse(item);
            // If the job status is LAUNCHING_ERROR, it is converted to ERROR
            if (JobStatusEnum.LAUNCHING_ERROR == resp.getCurrentStatus()) {
                resp.setLaunchingError(true);
                resp.setCurrentStatus(JobStatusEnum.ERROR);
            }
            val jobId = StreamingUtils.getJobId(resp.getModelId(), resp.getJobType().name());
            if (dataLatenciesMap != null && dataLatenciesMap.containsKey(jobId)) {
                resp.setDataLatency(convertDataLatency(dataLatenciesMap.get(jobId)));
            }
            val recordMgr = StreamingJobRecordManager.getInstance();
            val record = recordMgr.getLatestOneByJobId(jobId);
            if (record != null) {
                resp.setLastStatusDuration(System.currentTimeMillis() - record.getCreateTime());
            }
            return resp;
        }).collect(Collectors.toList());

        Comparator<StreamingJobResponse> comparator = propertyComparator(
                StringUtils.isEmpty(jobFilter.getSortBy()) ? "last_modified" : jobFilter.getSortBy(),
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
        if (targetList != null) {
            Map<String, NDataModel> modelMap = getAllDataModels(jobFilter.getProject());
            targetList.stream().forEach(entry -> {
                val id = entry.getId();
                val uuid = id.substring(0, id.lastIndexOf("_"));
                val dataModel = modelMap.get(uuid);
                if (dataModel != null) {
                    if (dataModel.isBroken() || isBatchModelBroken(dataModel)) {
                        entry.setModelBroken(true);
                    } else {
                        val mgr = indexPlanService.getManager(NIndexPlanManager.class, entry.getProject());
                        entry.setModelIndexes(mgr.getIndexPlan(uuid).getAllLayouts().size());
                        entry.setPartitionDesc(dataModel.getPartitionDesc());
                    }
                }
            });
        }
        return new DataResult<>(targetList, filterList.size(), offset, limit);
    }

    private List<ProjectInstance> getAllProjects(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        val prjMgr = NProjectManager.getInstance(config);
        if (!StringUtils.isEmpty(project)) {
            return Arrays.asList(prjMgr.getProject(project));
        } else {
            return prjMgr.listAllProjects();
        }
    }

    private Map<String, NDataModel> getAllDataModels(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelMap = new HashMap<String, NDataModel>();
        getAllProjects(project).stream().forEach(instance -> {
            val modelMgr = NDataModelManager.getInstance(config, instance.getName());
            val modelList = modelMgr.listAllModels();
            if (CollectionUtils.isNotEmpty(modelList)) {
                modelList.stream().forEach(model -> modelMap.put(model.getUuid(), model));
            }
        });
        return modelMap;
    }

    List<StreamingJobMeta> getAllStreamingJobsById(String project, List<String> jobIds) {
        if (CollectionUtils.isEmpty(jobIds)) {
            return Collections.emptyList();
        }

        if (StringUtils.isEmpty(project)) {
            throw new KylinException(INVALID_PARAMETER, "project is required when filter by jobid.");
        }

        val config = KylinConfig.getInstanceFromEnv();
        return jobIds.stream().map(StreamingJobManager.getInstance(config, project)::getStreamingJobByUuid)
                .collect(Collectors.toList());
    }

    private List<StreamingJobMeta> getAllStreamingJobs(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        val jobList = new ArrayList<StreamingJobMeta>();
        getAllProjects(project).stream().forEach(instance -> {
            val mgr = StreamingJobManager.getInstance(config, instance.getName());
            jobList.addAll(mgr.listAllStreamingJobMeta());
        });
        return jobList;
    }

    public boolean isBatchModelBroken(NDataModel dataModel) {
        try {
            if (dataModel.isFusionModel()) {
                val fmMgr = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), dataModel.getProject());
                val batchModel = fmMgr.getFusionModel(dataModel.getFusionId()).getBatchModel();
                return batchModel.isBroken();
            } else {
                return false;
            }
        } catch (Exception e) {
            return true;
        }
    }

    public StreamingJobDataStatsResponse getStreamingJobDataStats(String jobId, Integer timeFilter) {
        val resp = new StreamingJobDataStatsResponse();
        Message msg = MsgPicker.getMsg();
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        if (timeFilter > 0) {
            long startTime;
            if (timeFilter > 7 * 24 * 60) {
                throw new KylinException(INVALID_PARAMETER, msg.getILLEGAL_TIME_FILTER());
            } else {
                calendar.add(Calendar.MINUTE, -timeFilter);
                startTime = calendar.getTimeInMillis();
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
                minDataLatencyList.add(convertDataLatency(item.getMinDataLatency()));
                createDateList.add(item.getCreateTime());
            });
            resp.setConsumptionRateHist(consumptionRateList);
            resp.setProcessingTimeHist(procTimeList);
            resp.setDataLatencyHist(minDataLatencyList);
            resp.setCreateTime(createDateList);
        }
        return resp;
    }

    /**
     * convert minus data to zero
     * @param dataLatency
     * @return
     */
    private Long convertDataLatency(Long dataLatency) {
        return (dataLatency != null && dataLatency < 0) ? 0 : dataLatency;
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
        val recordMgr = StreamingJobRecordManager.getInstance();
        val record = recordMgr.getLatestOneByJobId(jobId);
        if (record != null) {
            resp.setLastStatusDuration(System.currentTimeMillis() - record.getCreateTime());
        }
        return resp;
    }

    /**
     * Gets the driver log InputStream of the stream Job ID
     */
    public InputStream getStreamingJobAllLog(String project, String jobId) {
        aclEvaluate.checkProjectOperationPermission(project);
        NExecutableManager executableManager = getManager(NExecutableManager.class, project);
        return executableManager.getStreamingOutputFromHDFS(jobId, Integer.MAX_VALUE).getVerboseMsgStream();
    }

    /**
     * Gets the driver simple log string of the stream Job ID
     */
    public String getStreamingJobSimpleLog(String project, String jobId) {
        aclEvaluate.checkProjectOperationPermission(project);
        NExecutableManager executableManager = getManager(NExecutableManager.class, project);
        return executableManager.getStreamingOutputFromHDFS(jobId).getVerboseMsg();
    }

    public void checkJobExecutionId(String project, String jobId, Integer execId) {
        val meta = getManager(StreamingJobManager.class, project).getStreamingJobByUuid(jobId);
        val msg = String.format(Locale.ROOT, "JobExecutionId(%d) is invalid", execId);
        Preconditions.checkState(ObjectUtils.equals(execId, meta.getJobExecutionId()), msg);
    }
}
