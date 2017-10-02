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

package io.kyligence.kap.source.hive.modelstats;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckDataSkewStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(CheckDataSkewStep.class);
    public final static String MODEL_NAME = "model_name";

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final BufferedLogger stepLogger = new BufferedLogger(logger);
        String modelName = getParam(MODEL_NAME);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(kylinConfig);

        try {
            ModelStats modelStats = modelStatsManager.getModelStats(modelName);
            DataModelDesc dataModelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc(modelName);
            ModelDiagnose.checkDataSkewOnFactTable(dataModelDesc, modelStats, kylinConfig);
            stepLogger.log("Success");
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());
        } catch (IOException e) {
            stepLogger.log("Failed to check data skew: " + e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
        }
    }
}
