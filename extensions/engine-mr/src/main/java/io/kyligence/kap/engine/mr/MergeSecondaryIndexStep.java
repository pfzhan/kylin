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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import java.io.IOException;
import java.util.*;

public class MergeSecondaryIndexStep extends AbstractExecutable {

    public MergeSecondaryIndexStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment newSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        final List<CubeSegment> mergingSegments = cube.getMergingSegments(newSegment);
        KylinConfig conf = cube.getConfig();

        Collections.sort(mergingSegments);

        try {

            mergeIndex(conf, cube, newSegment, mergingSegments);

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to merge dictionary or lookup snapshots", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    /*
     * @param cube
     * @param newSeg
     * @throws IOException
     */
    private void mergeIndex(KylinConfig conf, CubeInstance cube, CubeSegment newSeg, List<CubeSegment> mergingSegments) throws IOException {

    }

}
