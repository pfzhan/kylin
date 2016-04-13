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
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCubingJobBuilder2 extends org.apache.kylin.engine.mr.BatchCubingJobBuilder2 {
    private static final Logger logger = LoggerFactory.getLogger(BatchCubingJobBuilder2.class);

    public BatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
    }

    @Override
    protected void addOtherStepBeforeCubing(CubingJob result) {

        if (((CubeSegment) seg).getCubeDesc().getRowkey().getColumnsNeedIndex().length > 0) {
            MapReduceExecutable task = createBuildSecondaryIndexStep(result.getId());
            result.addTask(task);
        }
    }

    private MapReduceExecutable createBuildSecondaryIndexStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Build Secondary Index");
        result.setMapReduceJobClass(SecondaryIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, ((CubeSegment) seg).getCubeDesc().getModel());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getSecondaryIndexPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Second_Index_" + seg.getRealization().getName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }


}
