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

package io.kyligence.kap.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.IBatchCubingEngine;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

import io.kyligence.kap.cube.model.DataModelFlatTableDesc;

public class KapMRBatchCubingEngine implements IBatchCubingEngine {

    @Override
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeDesc cubeDesc) {
        return new CubeJoinedFlatTableDesc(cubeDesc);
    }

    @Override
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeSegment newSegment) {
        return new DataModelFlatTableDesc(newSegment);
    }

    @Override
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        return new KapBatchCubingJobBuilder(newSegment, submitter).build();
    }

    @Override
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter) {
        return new KapBatchMergeJobBuilder(mergeSegment, submitter).build();
    }

    @Override
    public Class<?> getSourceInterface() {
        return IMRInput.class;
    }

    @Override
    public Class<?> getStorageInterface() {
        return IMROutput2.class;
    }

}
