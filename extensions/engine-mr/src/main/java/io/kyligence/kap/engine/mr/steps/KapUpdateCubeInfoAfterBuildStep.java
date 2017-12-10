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

package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.mp.MPCubeManager;

public class KapUpdateCubeInfoAfterBuildStep extends UpdateCubeInfoAfterBuildStep {
    private static final Logger logger = LoggerFactory.getLogger(KapUpdateCubeInfoAfterBuildStep.class);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        ExecuteResult result = super.doWork(context);
        if (!result.succeed())
            return result;
        
        final MPCubeManager mpMgr = MPCubeManager.getInstance(context.getConfig());
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        
        if (!mpMgr.isMPCube(cube))
            return result;
        
        // For MP cube, we keep all MP cubes DISABLED and turn its master to READY.
        // By keeping all MP cubes DISABLED, they are ignored by query naturally, and let the master represent.
        CubeInstance master = mpMgr.convertToMPMasterIfNeeded(cube.getName());
        try {
            // disable MP Cube
            cubeManager.updateCubeStatus(cube, RealizationStatusEnum.DISABLED);
            
            // enable MP Master
            if (master.getStatus() == RealizationStatusEnum.DISABLED) {
                cubeManager.updateCubeStatus(master, RealizationStatusEnum.READY);
            }
        } catch (IOException e) {
            logger.error("Failed to update MP master " + master + " status to ready", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
        
        return result;
    }
}
