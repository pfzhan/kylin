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

import java.util.Set;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;

/**
 */
public class NSparkCubingJob extends DefaultChainedExecutable {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingJob.class);

    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<NCuboidLayout> layouts, String submitter) {
        Preconditions.checkArgument(segments.size() > 0);
        Preconditions.checkArgument(layouts.size() > 0);
        Preconditions.checkArgument(submitter != null);

        NSparkCubingJob job = new NSparkCubingJob();
        job.setSubmitter(submitter);

        job.addSparkCubingStep(segments, layouts);

        job.addUpdateAfterBuildStep();

        return job;
    }

    public NSparkCubingStep getSparkCubingStep() {
        return (NSparkCubingStep) getTasks().get(0);
    }

    private void addSparkCubingStep(Set<NDataSegment> segments, Set<NCuboidLayout> layouts) {
        NSparkCubingStep step = new NSparkCubingStep();
        NDataflow df = segments.iterator().next().getDataflow();
        KylinConfigExt config = df.getConfig();

        step.setDataflowName(df.getName());
        step.setSegmentIds(NSparkCubingUtil.toSegmentIds(segments));
        step.setCuboidLayoutIds(NSparkCubingUtil.toCuboidLayoutIds(layouts));
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(step.getId()).toString());
        step.setJobId(getId());
        this.addTask(step);
    }

    private void addUpdateAfterBuildStep() {
        this.addTask(new NSparkCubingUpdateAfterBuildStep());
    }

}
