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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;

public class NSparkCleanupAfterMergeStep extends AbstractExecutable {

    public void setSegmentIds(Set<Integer> segmentIds) {
        this.setParam(NBatchConstants.P_SEGMENT_IDS, NSparkCubingUtil.ids2Str(segmentIds));
    }

    public void setDataflowName(String dataflowName) {
        this.setParam(NBatchConstants.P_DATAFLOW_NAME, dataflowName);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        String name = getParam(NBatchConstants.P_DATAFLOW_NAME);
        Set<Integer> segmentIds = NSparkCubingUtil.str2Ints(getParam(NBatchConstants.P_SEGMENT_IDS));
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflow dataflow = NDataflowManager.getInstance(config, getProject()).getDataflow(name);
        String hdfsWorkingDir = KapConfig.wrap(config).getReadHdfsWorkingDirectory();

        for (Integer segmentId : segmentIds) {
            String path = hdfsWorkingDir + "parquet/" + dataflow.getUuid() + "/" + segmentId;

            try {
                HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(path));
            } catch (IOException e) {
                throw new ExecuteException("Can not delete segment: " + segmentId + ", in dataflow: " + name);
            }
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED);
    }

}
