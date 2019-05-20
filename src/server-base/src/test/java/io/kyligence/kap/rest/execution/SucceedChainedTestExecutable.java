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

package io.kyligence.kap.rest.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;

/**
 */
public class SucceedChainedTestExecutable extends DefaultChainedExecutable {

    public SucceedChainedTestExecutable() {
        super();
        this.initConfig(KylinConfig.getInstanceFromEnv());
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Thread.sleep(1000);
            this.retry++;
        } catch (InterruptedException e) {
        }
        return ExecuteResult.createSucceed();
    }

    @Override
    public boolean needRetry() {
        return super.needRetry();
    }

    @Override
    public void cancelJob() throws IOException {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        segments.add(dataflow.getSegments().getFirstSegment());
        NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
        NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
    }
}
