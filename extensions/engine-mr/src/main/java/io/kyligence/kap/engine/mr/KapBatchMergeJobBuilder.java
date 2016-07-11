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

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.BatchMergeJobBuilder2;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;

import io.kyligence.kap.engine.mr.steps.KapMergeCuboidJob;
import io.kyligence.kap.engine.mr.steps.MergeSecondaryIndexStep;

public class KapBatchMergeJobBuilder extends BatchMergeJobBuilder2 {

    public KapBatchMergeJobBuilder(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
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

    protected Class<? extends AbstractHadoopJob> getMergeCuboidJob() {
        return KapMergeCuboidJob.class;
    }
}
