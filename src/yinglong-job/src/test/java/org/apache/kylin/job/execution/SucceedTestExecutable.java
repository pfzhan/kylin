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

package org.apache.kylin.job.execution;

import java.util.HashMap;
import java.util.Map;

import io.kyligence.kap.job.JobContext;

/**
 */
public class SucceedTestExecutable extends BaseTestExecutable {

    public SucceedTestExecutable() {
        super();
    }

    public SucceedTestExecutable(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected ExecuteResult doWork(JobContext context) {
        Map<String, String> info = new HashMap<String, String>(){{put("runningStatus", "inRunning");}};
        updateJobOutput(getProject(), getId(), ExecutableState.RUNNING, info, null, null);
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
}
