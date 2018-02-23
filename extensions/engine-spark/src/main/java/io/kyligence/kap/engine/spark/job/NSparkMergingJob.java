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
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;

public class NSparkMergingJob extends DefaultChainedExecutable {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkMergingJob.class);

    /**
     * Merge the segments that are contained in the given mergedSegment
     *
     * @param mergedSegment, new segment that expect to merge, which should contains a couple of ready segments.
     * @param layouts,       user is allowed to specify the cuboids to merge. By default, it is null and merge all
     *                       the ready cuboids in the segments.
     */
    public static NSparkMergingJob merge(NDataSegment mergedSegment, Set<NCuboidLayout> layouts, String submitter) {
        Preconditions.checkArgument(mergedSegment != null);
        Preconditions.checkArgument(submitter != null);

        NSparkMergingJob job = new NSparkMergingJob();
        job.setSubmitter(submitter);
        job.addSparkMergingStep(mergedSegment, layouts);
        job.addUpdateAfterMergeStep();
        job.addCleanupStep(mergedSegment);

        return job;
    }

    public NSparkMergingStep getSparkCubingStep() {
        return (NSparkMergingStep) getTasks().get(0);
    }

    private void addSparkMergingStep(NDataSegment mergedSegment, Set<NCuboidLayout> layouts) {
        NSparkMergingStep step = new NSparkMergingStep();
        NDataflow df = mergedSegment.getDataflow();
        KylinConfigExt config = df.getConfig();
        step.setDataflowName(df.getName());
        step.setSegmentId(mergedSegment.getId());
        step.setCuboidLayoutIds(NSparkCubingUtil.toCuboidLayoutIds(layouts));
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(step.getId()).toString());
        step.setJobId(getId());
        this.addTask(step);
    }

    private void addUpdateAfterMergeStep() {
        this.addTask(new NSparkCubingUpdateAfterMergeStep());
    }

    private void addCleanupStep(NDataSegment mergedSegment) {
        NDataflow dataflow = mergedSegment.getDataflow();
        String name = dataflow.getName();
        Collection<Integer> ids = Collections2.transform(dataflow.getMergingSegments(mergedSegment),
                new Function<NDataSegment, Integer>() {
                    @Nullable
                    @Override
                    public Integer apply(@Nullable NDataSegment input) {
                        return input.getId();
                    }
                });

        Set<Integer> segmentIds = new HashSet<>();
        segmentIds.addAll(ids);

        NSparkCleanupAfterMergeStep step = new NSparkCleanupAfterMergeStep();
        step.setDataflowName(name);
        step.setSegmentIds(segmentIds);
        this.addTask(step);
    }
}
