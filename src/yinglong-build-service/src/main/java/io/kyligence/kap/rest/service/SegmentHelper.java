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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("segmentHelper")
public class SegmentHelper extends BasicService implements SegmentHelperSupporter {

    private static final Logger logger = LoggerFactory.getLogger(SegmentHelper.class);

    public void refreshRelatedModelSegments(String project, String tableName, SegmentRange toBeRefreshSegmentRange)
            throws IOException {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(tableName);
        if (tableDesc == null) {
            throw new IllegalArgumentException("TableDesc '" + tableName + "' does not exist");
        }

        val models = NDataflowManager.getInstance(kylinConfig, project).getTableOrientedModelsUsingRootTable(tableDesc);
        boolean first = true;
        List<SegmentRange> firstRanges = Lists.newArrayList();

        val loadingRange = NDataLoadingRangeManager.getInstance(kylinConfig, project).getDataLoadingRange(tableName);

        if (CollectionUtils.isNotEmpty(models)) {

            val jobManager = getManager(JobManager.class, project);
            for (val model : models) {
                val modelId = model.getUuid();
                IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project).getIndexPlan(modelId);
                NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
                NDataflow df = dfMgr.getDataflow(indexPlan.getUuid());
                Segments<NDataSegment> segments = df.getSegmentsByRange(toBeRefreshSegmentRange);
                List<SegmentRange> ranges = Lists.newArrayList();

                if (RealizationStatusEnum.LAG_BEHIND != df.getStatus()) {
                    if (CollectionUtils.isEmpty(segments) && loadingRange == null) {
                        logger.info("Refresh model {} without partition key, but it does not exist, build it.",
                                modelId);
                        //Full build segment to refresh does not exist, build it.
                        buildFullSegment(model.getUuid(), project);
                        continue;
                    } else {
                        //normal model to refresh must has ready segment
                        Preconditions.checkState(CollectionUtils.isNotEmpty(segments));
                        refreshSegments(segments, dfMgr, df, modelId, jobManager, project);
                        ranges.addAll(segments.stream().map(NDataSegment::getSegRange).collect(Collectors.toList()));
                    }
                } else {
                    refreshSegments(segments.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING), dfMgr, df,
                            modelId, jobManager, project);
                    ranges.addAll(segments.stream().map(NDataSegment::getSegRange).collect(Collectors.toList()));
                    //remove new segment in lag behind models and then rebuild it
                    handleRefreshLagBehindModel(project, df, segments.getSegments(SegmentStatusEnum.NEW), modelId,
                            dfMgr, jobManager);
                }
                //check range in every model to refresh same
                if (first) {
                    firstRanges = ranges;
                    first = false;
                } else {
                    Preconditions.checkState(firstRanges.equals(ranges));
                }

            }
        }
    }

    private void refreshSegments(Segments<NDataSegment> segments, NDataflowManager dfMgr, NDataflow df, String modelId,
            JobManager jobManager, String project) {
        for (NDataSegment seg : segments) {
            NDataSegment newSeg = dfMgr.refreshSegment(df, seg.getSegRange());
            getManager(SourceUsageManager.class).licenseCheckWrap(project, () -> {
                jobManager.refreshSegmentJob(new JobParam(newSeg, modelId, getUsername()));
                return null;
            });
        }
    }

    private void buildFullSegment(String model, String project) {
        val jobManager = getManager(JobManager.class, project);
        val dataflowManager = getManager(NDataflowManager.class, project);
        val indexPlanManager = getManager(NIndexPlanManager.class, project);
        val indexPlan = indexPlanManager.getIndexPlan(model);
        val dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        val newSegment = dataflowManager.appendSegment(dataflow,
                new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
        getManager(SourceUsageManager.class).licenseCheckWrap(project,
                () -> jobManager.addSegmentJob(new JobParam(newSegment, model, getUsername())));
    }

    private void handleRefreshLagBehindModel(String project, NDataflow df, Segments<NDataSegment> newSegments,
            String modelId, NDataflowManager dfMgr, JobManager jobManager) throws IOException {
        //if new segment missed, do nothing
        for (NDataSegment seg : newSegments) {
            handleJobAndOldSeg(project, seg, df, dfMgr);
            df = dfMgr.getDataflow(modelId);
            val newSeg = dfMgr.appendSegment(df, seg.getSegRange());
            getManager(SourceUsageManager.class).licenseCheckWrap(project,
                    () -> jobManager.addSegmentJob(new JobParam(newSeg, modelId, getUsername())));
        }
    }

    private void handleJobAndOldSeg(String project, NDataSegment seg, NDataflow df, NDataflowManager dfMgr)
            throws IOException {
        val jobManager = getManager(NExecutableManager.class, project);
        val jobs = jobManager.getAllExecutables();
        var segmentDeleted = false;
        for (val job : jobs) {
            if (!job.getStatus().isFinalState()) {
                if (job.getTargetSegments().contains(seg.getId())) {
                    logger.info("Cancel and discard the job {} related with segment {}.", job.getId(), seg.getId());
                    jobManager.discardJob(job.getId());
                    segmentDeleted = true;
                }
            }
        }
        if (!segmentDeleted) {
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(Lists.newArrayList(seg).toArray(new NDataSegment[0]));
            dfMgr.updateDataflow(update);
        }
        logger.info("Drop segment {} and rebuild it immediately.", seg.getId());
    }

    @Override
    public void removeSegment(String project, String dataflowId, Set<String> tobeRemoveSegmentIds) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(dataflowId);
        if (CollectionUtils.isEmpty(tobeRemoveSegmentIds)) {
            return;
        }

        List<NDataSegment> dataSegments = Lists.newArrayList();
        for (String tobeRemoveSegmentId : tobeRemoveSegmentIds) {
            NDataSegment dataSegment = df.getSegment(tobeRemoveSegmentId);
            if (dataSegment == null) {
                continue;
            }
            dataSegments.add(dataSegment);
        }

        if (CollectionUtils.isNotEmpty(dataSegments)) {
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(dataSegments.toArray(new NDataSegment[0]));
            dfMgr.updateDataflow(update);
        }

    }
}
