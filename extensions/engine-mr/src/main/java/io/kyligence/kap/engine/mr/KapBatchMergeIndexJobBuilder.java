/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package io.kyligence.kap.engine.mr;

import java.util.List;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.BatchMergeJobBuilder2;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.engine.mr.steps.KapMergeCuboidJob;
import io.kyligence.kap.engine.mr.steps.MergeSecondaryIndexStep;

public class KapBatchMergeIndexJobBuilder extends BatchMergeJobBuilder2 {

    private static final Logger logger = LoggerFactory.getLogger(KapBatchMergeIndexJobBuilder.class);

    private final IMROutput2.IMRBatchMergeOutputSide2 outputSide;

    public KapBatchMergeIndexJobBuilder(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
        this.outputSide = MRUtil.getBatchMergeOutputSide2(seg);
    }

    @SuppressWarnings("unused")
    private MergeSecondaryIndexStep createMergeSecondaryIndexStep(CubingJob cubingJob) {
        MergeSecondaryIndexStep result = new MergeSecondaryIndexStep();
        result.setName("Merge Secondary Index");

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setIndexPath(this.getSecondaryIndexPath(cubingJob.getId()), result.getParams());
        return result;
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to MERGE segment " + seg);

        final CubeSegment cubeSegment = seg;
        final CubingJob result = CubingJob.createMergeJob(cubeSegment, submitter, config);
        final String jobId = result.getId();

        final List<CubeSegment> mergingSegments = cubeSegment.getCubeInstance().getMergingSegments(cubeSegment);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingSegmentIds = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
        }
        // here merge rawTableSegment and parquet files
        outputSide.addStepPhase2_BuildCube(seg, mergingSegments, result);
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));
        outputSide.addStepPhase3_Cleanup(result);

        return result;
    }

    protected Class<? extends AbstractHadoopJob> getMergeCuboidJob() {
        return KapMergeCuboidJob.class;
    }
}
