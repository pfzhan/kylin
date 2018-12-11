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

import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class SucceedTestExecutable extends DefaultChainedExecutable {

    public SucceedTestExecutable() {
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
        NDataflow dataflow = nDataflowManager.getDataflow("ncube_basic");
        List<NDataSegment> segments = new ArrayList<>();
        segments.add(dataflow.getSegments().getFirstSegment());
        NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
        NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
    }
}
