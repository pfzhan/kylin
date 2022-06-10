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
package io.kyligence.kap.engine.spark;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.ChainedExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.ExecutableUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.Assert;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.execution.NSparkCubingJob;
import io.kyligence.kap.job.execution.step.NSparkCubingStep;
import io.kyligence.kap.job.execution.merger.AfterBuildResourceMerger;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.job.JobBucket;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

public class IndexDataConstructor {

    private String project;

    public IndexDataConstructor(String project) {
        this.project = project;
    }

    private int buildCount = 0;

    public static ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            ExecutableState status = job.getStatus();
            if (!status.isProgressing()) {
                return status;
            }
        }
    }

    public static boolean wait(List<? extends AbstractExecutable> jobs) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            val isFinished = jobs.stream().map(j -> !j.getStatus().isProgressing()).reduce(true,
                    (left, right) -> left && right);
            if (isFinished) {
                return jobs.stream().map(j -> j.getStatus() == ExecutableState.SUCCEED).reduce(true,
                        (left, right) -> left && right);
            }
        }
    }

    public static String firstFailedJobErrorMessage(ExecutableManager execMgr, ChainedExecutable job) {
        return job.getTasks().stream()
                .filter(abstractExecutable -> abstractExecutable.getStatus() == ExecutableState.ERROR).findFirst()
                .map(task -> execMgr.getOutputFromHDFSByJobId(job.getId(), task.getId(), Integer.MAX_VALUE)
                        .getVerboseMsg())
                .orElse("Unknown Error");
    }

    public void buildDataflow(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(round1),
                true);
    }

    public void buildMultiSegmentPartitions(String dfName, String segStart, String segEnd, List<Long> layoutIds,
            List<Long> partitionIds) throws Exception {
        val config = NLocalFileMetadataTestCase.getTestConfig();
        val dfManager = NDataflowManager.getInstance(config, project);
        val df = dfManager.getDataflow(dfName);
        val partitionValues = df.getModel().getMultiPartitionDesc().getPartitionValuesById(partitionIds);

        // append segment
        long start = SegmentRange.dateToLong(segStart);
        long end = SegmentRange.dateToLong(segEnd);
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment = dfManager.appendSegment(df, segmentRange, SegmentStatusEnum.NEW, partitionValues);

        Set<LayoutEntity> layouts = Sets.newHashSet();
        IndexPlan indexPlan = df.getIndexPlan();
        for (Long id : layoutIds) {
            layouts.add(indexPlan.getLayoutEntity(id));
        }
        buildSegment(dfName, dataSegment, layouts, true, partitionValues);
    }

    public void buildIndex(String dfName, SegmentRange segmentRange, Set<LayoutEntity> toBuildLayouts, boolean isAppend)
            throws Exception {
        buildIndex(dfName, segmentRange, toBuildLayouts, isAppend, null);
    }

    // return segment id
    public String buildIndex(String dfName, SegmentRange segmentRange, Set<LayoutEntity> toBuildLayouts,
            boolean isAppend, List<String[]> partitionValues) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, segmentRange, SegmentStatusEnum.NEW, partitionValues);
        buildSegment(dfName, oneSeg, toBuildLayouts, isAppend, partitionValues);
        return oneSeg.getId();
    }

    public void buildSegment(String dfName, NDataSegment segment, Set<LayoutEntity> toBuildLayouts, boolean isAppend,
            List<String[]> partitionValues) throws InterruptedException {
        buildSegments(Lists.newArrayList(new BuildInfo(dfName, segment, toBuildLayouts, isAppend, partitionValues)));
    }

    public void buildSegments(List<BuildInfo> buildInfos) throws InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val indexDataRepo = new IndexDataWarehouse(config, project, buildCount + "");
        if (indexDataRepo.reuseBuildData()) {
            buildCount++;
            return;
        }
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);

        List<NSparkCubingJob> jobs = Lists.newArrayList();
        for (BuildInfo buildInfo : buildInfos) {
            val dfName = buildInfo.dataflowId;
            val partitionValues = buildInfo.partitionValues;
            val segment = buildInfo.segment;
            val toBuildLayouts = buildInfo.toBuildLayouts;
            NDataflow df = dsMgr.getDataflow(dfName);
            Set<JobBucket> buckets = Sets.newHashSet();
            if (CollectionUtils.isNotEmpty(partitionValues)) {
                NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
                Set<Long> targetPartitions = modelManager.getDataModelDesc(dfName).getMultiPartitionDesc()
                        .getPartitionIdsByValues(partitionValues);
                val bucketStart = new AtomicLong(segment.getMaxBucketId());
                toBuildLayouts.forEach(layout -> {
                    targetPartitions.forEach(partition -> {
                        buckets.add(new JobBucket(segment.getId(), layout.getId(), bucketStart.incrementAndGet(),
                                partition));
                    });
                });
                dsMgr.updateDataflow(df.getId(),
                        copyForWrite -> copyForWrite.getSegment(segment.getId()).setMaxBucketId(bucketStart.get()));
            }
            NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), toBuildLayouts, "ADMIN", buckets);
            if (buildInfo.isAppend) {
                job.setJobType(JobTypeEnum.INC_BUILD);
            } else {
                job.setJobType(JobTypeEnum.INDEX_BUILD);
            }
            NSparkCubingStep sparkStep = job.getSparkCubingStep();
            StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
            Assert.assertEquals("hdfs", distMetaUrl.getScheme());
            Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

            // launch the job
            execMgr.addJob(job);
            jobs.add(job);
        }
        if (!wait(jobs)) {
            throw new IllegalStateException(firstFailedJobErrorMessage(execMgr, jobs.get(0)));
        }
        for (val job : jobs) {
            val merger = new AfterBuildResourceMerger(config, project);
            val sparkStep = job.getSparkCubingStep();
            merger.merge(job.getTargetModelId(), job.getSegmentIds(), ExecutableUtils.getLayoutIds(sparkStep),
                    ExecutableUtils.getRemoteStore(config, sparkStep), job.getJobType(), job.getTargetPartitions());
        }
        indexDataRepo.persistBuildData();
        buildCount++;
    }

    public void buildMultiPartition(List<BuildInfo> buildInfos) throws InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val indexDataRepo = new IndexDataWarehouse(config, project, buildCount + "");
        if (indexDataRepo.reuseBuildData()) {
            buildCount++;
            return;
        }
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, project);

        List<NSparkCubingJob> jobs = Lists.newArrayList();
        for (BuildInfo buildInfo : buildInfos) {
            val dfName = buildInfo.dataflowId;
            val partitionValues = buildInfo.partitionValues;
            val segment = buildInfo.segment;
            val toBuildLayouts = buildInfo.toBuildLayouts;
            NDataflow df = dsMgr.getDataflow(dfName);
            Set<JobBucket> buckets = Sets.newHashSet();
            if (CollectionUtils.isNotEmpty(partitionValues)) {
                NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
                Set<Long> targetPartitions = modelManager.getDataModelDesc(dfName).getMultiPartitionDesc()
                        .getPartitionIdsByValues(partitionValues);
                val bucketStart = new AtomicLong(segment.getMaxBucketId());
                toBuildLayouts.forEach(layout -> {
                    targetPartitions.forEach(partition -> {
                        buckets.add(new JobBucket(segment.getId(), layout.getId(), bucketStart.incrementAndGet(),
                                partition));
                    });
                });
                dsMgr.updateDataflow(df.getId(),
                        copyForWrite -> copyForWrite.getSegment(segment.getId()).setMaxBucketId(bucketStart.get()));
            }
            NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), toBuildLayouts, "ADMIN", buckets);
            job.setJobType(JobTypeEnum.SUB_PARTITION_BUILD);
            NSparkCubingStep sparkStep = job.getSparkCubingStep();
            StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
            Assert.assertEquals("hdfs", distMetaUrl.getScheme());
            Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

            // launch the job
            execMgr.addJob(job);
            jobs.add(job);
        }
        if (!wait(jobs)) {
            throw new IllegalStateException(firstFailedJobErrorMessage(execMgr, jobs.get(0)));
        }
        for (val job : jobs) {
            val merger = new AfterBuildResourceMerger(config, project);
            val sparkStep = job.getSparkCubingStep();
            merger.merge(job.getTargetModelId(), job.getSegmentIds(), ExecutableUtils.getLayoutIds(sparkStep),
                    ExecutableUtils.getRemoteStore(config, sparkStep), job.getJobType(), job.getTargetPartitions());
        }
        indexDataRepo.persistBuildData();
        buildCount++;
    }

    public void buildMultiPartition(String dfName, String segmentId, Set<LayoutEntity> toBuildLayouts, boolean isAppend,
            List<String[]> partitionValues) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        // ready dataflow, segment, cuboid layout
        dsMgr.appendPartitions(df.getId(), segmentId, partitionValues);
        NDataSegment segment = df.getSegment(segmentId);
        buildMultiPartition(
                Lists.newArrayList(new BuildInfo(dfName, segment, toBuildLayouts, isAppend, partitionValues)));
    }

    @Data
    @AllArgsConstructor
    public static class BuildInfo {
        String dataflowId;
        NDataSegment segment;
        Set<LayoutEntity> toBuildLayouts;
        boolean isAppend;
        List<String[]> partitionValues;
    }
}
