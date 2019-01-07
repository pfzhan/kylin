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

package io.kyligence.kap.engine.spark.job;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Collections2;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.builder.NModelAnalysisJob;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;

public class JobStepFactoryTest extends NLocalWithSparkSessionTest {
    private KylinConfig config;

    @Before
    public void setup() throws Exception {
        config = getTestConfig();
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testAddStepInCubing() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        Set<NDataSegment> segments = Sets.newHashSet(oneSeg);
        Set<LayoutEntity> layouts = Sets.newHashSet(df.getIndexPlan().getAllLayouts());
        NSparkCubingJob job = NSparkCubingJob.create(segments, layouts, "ADMIN");
        NSparkExecutable resourceDetectStep = JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, segments,
                layouts);
        Assert.assertTrue(resourceDetectStep instanceof NResourceDetectStep);
        Assert.assertEquals(ResourceDetectBeforeCubingJob.class.getName(),
                resourceDetectStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_DETECT_RESOURCE, resourceDetectStep.getName());
        compareParameter(resourceDetectStep, segments, layouts, job);

        NSparkExecutable analysisStep = JobStepFactory.addStep(job, JobStepType.ANALYSIS, segments, layouts);
        Assert.assertTrue(analysisStep instanceof NSparkAnalysisStep);
        Assert.assertEquals(NModelAnalysisJob.class.getName(), analysisStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_DATA_PROFILING, analysisStep.getName());
        compareParameter(analysisStep, segments, layouts, job);

        NSparkExecutable cubeStep = JobStepFactory.addStep(job, JobStepType.CUBING, segments, layouts);
        Assert.assertTrue(cubeStep instanceof NSparkCubingStep);
        Assert.assertEquals(config.getSparkBuildClassName(), cubeStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE, cubeStep.getName());
        compareParameter(cubeStep, segments, layouts, job);
    }

    @Test
    public void testAddStepInMerging() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow flowCopy = dsMgr.getDataflow(df.getUuid()).copy();

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        NDataSegment firstSeg = new NDataSegment();
        firstSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-02"),
                SegmentRange.dateToLong("2011-01-01")));
        firstSeg.setStatus(SegmentStatusEnum.READY);
        firstSeg.setId(UUID.randomUUID().toString());

        NDataSegment secondSeg = new NDataSegment();
        secondSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2011-01-01"),
                SegmentRange.dateToLong("2013-01-01")));
        secondSeg.setStatus(SegmentStatusEnum.READY);
        secondSeg.setId(UUID.randomUUID().toString());

        Segments<NDataSegment> mergingSegments = new Segments<>();
        mergingSegments.add(firstSeg);
        mergingSegments.add(secondSeg);
        flowCopy.setSegments(mergingSegments);

        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        NDataSegment firstMergeSeg = dsMgr.mergeSegments(flowCopy, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-02"), SegmentRange.dateToLong("2013-01-01")), true);
        Set<NDataSegment> segments = Sets.newHashSet(firstMergeSeg);
        Set<LayoutEntity> layouts = Sets.newHashSet(flowCopy.getIndexPlan().getAllLayouts());
        NSparkMergingJob job = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                UUID.randomUUID().toString());

        NSparkExecutable resourceDetectStep = JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, segments,
                layouts);
        Assert.assertTrue(resourceDetectStep instanceof NResourceDetectStep);
        Assert.assertEquals(ResourceDetectBeforeMergingJob.class.getName(),
                resourceDetectStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_DETECT_RESOURCE, resourceDetectStep.getName());
        compareParameter(resourceDetectStep, segments, layouts, job);

        NSparkExecutable mergeStep = JobStepFactory.addStep(job, JobStepType.MERGING, segments, layouts);
        Assert.assertTrue(mergeStep instanceof NSparkMergingStep);
        Assert.assertEquals(config.getSparkMergeClassName(), mergeStep.getSparkSubmitClassName());
        Assert.assertEquals(ExecutableConstants.STEP_NAME_MERGER_SPARK_SEGMENT, mergeStep.getName());
        compareParameter(mergeStep, segments, layouts, job);

        NSparkExecutable cleanStep = JobStepFactory.addStep(job, JobStepType.CLEAN_UP_AFTER_MERGE, segments, layouts);
        Assert.assertTrue(cleanStep instanceof NSparkCleanupAfterMergeStep);
        Assert.assertEquals(ExecutableConstants.STEP_NAME_CLEANUP, cleanStep.getName());
        Assert.assertEquals(segments.iterator().next().getModel().getUuid(), cleanStep.getTargetModel());
        Assert.assertEquals(job.getId(), cleanStep.getParam(NBatchConstants.P_JOB_ID));
        Assert.assertEquals(segments.iterator().next().getDataflow().getUuid(), cleanStep.getDataflowId());
        Collection<String> ids = Collections2.transform(df.getMergingSegments(segments.iterator().next()),
                NDataSegment::getId);
        Assert.assertEquals(Sets.newHashSet(ids), cleanStep.getSegmentIds());
        Assert.assertEquals(NSparkCubingUtil.toCuboidLayoutIds(layouts), cleanStep.getCuboidLayoutIds());
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(cleanStep.getId()).toString(), cleanStep.getDistMetaUrl());
    }

    private void compareParameter(NSparkExecutable step, Set<NDataSegment> segments, Set<LayoutEntity> layouts,
            DefaultChainedExecutable job) {
        Assert.assertEquals(segments.iterator().next().getModel().getUuid(), step.getTargetModel());
        Assert.assertEquals(job.getId(), step.getParam(NBatchConstants.P_JOB_ID));
        Assert.assertEquals(segments.iterator().next().getDataflow().getUuid(), step.getDataflowId());
        Assert.assertEquals(NSparkCubingUtil.toSegmentIds(segments), step.getSegmentIds());
        Assert.assertEquals(NSparkCubingUtil.toCuboidLayoutIds(layouts), step.getCuboidLayoutIds());
        Assert.assertEquals(config.getJobTmpMetaStoreUrl(step.getId()).toString(), step.getDistMetaUrl());
    }
}
