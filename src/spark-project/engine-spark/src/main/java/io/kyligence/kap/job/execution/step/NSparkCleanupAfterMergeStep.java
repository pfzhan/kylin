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

package io.kyligence.kap.job.execution.step;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;

public class NSparkCleanupAfterMergeStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCleanupAfterMergeStep.class);

    public NSparkCleanupAfterMergeStep() {
        this.setName(ExecutableConstants.STEP_NAME_CLEANUP);
    }

    public NSparkCleanupAfterMergeStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected ExecuteResult doWork(JobContext context) throws ExecuteException {

        String name = getParam(NBatchConstants.P_DATAFLOW_ID);
        String[] segmentIds = StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS), ",");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflow dataflow = NDataflowManager.getInstance(config, getProject()).getDataflow(name);

        boolean timeMachineEnabled = KylinConfig.getInstanceFromEnv().getTimeMachineEnabled();

        for (String segmentId : segmentIds) {
            String path = dataflow.getSegmentHdfsPath(segmentId);
            if (!SecondStorageUtil.isModelEnable(dataflow.getProject(), dataflow.getModel().getUuid())) {
                if (!timeMachineEnabled) {
                    try {
                        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(path));
                        logger.info("The segment {} in dataflow {} has been successfully deleted, path : {}", //
                                segmentId, name, path);
                    } catch (IOException e) {
                        logger.warn("Can not delete segment {} in dataflow {}." + //
                                " Please try workaround thru garbage clean manually.", segmentId, name, e);
                    }
                }
            } else {
                logger.info("ClickHouse is enabled for the model, please delete segments {} in dataflow {} manually.", //
                        segmentIds, name);
            }
        }

        return ExecuteResult.createSucceed();
    }

}
