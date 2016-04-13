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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.MergeCuboidJob;
import org.apache.kylin.engine.mr.steps.MergeDictionaryStep;
import org.apache.kylin.engine.mr.steps.MergeStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BatchMergeJobBuilder2 extends org.apache.kylin.engine.mr.BatchMergeJobBuilder2 {

    public BatchMergeJobBuilder2(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
    }

    @Override
    protected void addOtherStepBeforeMerge(CubingJob cubingJob) {
        if (((CubeSegment) seg).getCubeDesc().getRowkey().getColumnsNeedIndex().length > 0) {
            MergeSecondaryIndexStep result = new MergeSecondaryIndexStep();
            result.setName("Merge Secondary Index");

            CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
            CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
            CubingExecutableUtil.setIndexPath(this.getSecondaryIndexPath(cubingJob.getId()), result.getParams());
            cubingJob.addTask(result);
        }
    }

}
