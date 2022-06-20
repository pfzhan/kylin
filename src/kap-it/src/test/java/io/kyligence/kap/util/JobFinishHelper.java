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

package io.kyligence.kap.util;

import static org.awaitility.Awaitility.with;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.awaitility.core.ConditionFactory;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;

public class JobFinishHelper {

    public static void waitJobFinish(KylinConfig config, String project, String jobId, long maxWaitMilliseconds) {
        ExecutableManager executableManager = ExecutableManager.getInstance(config, project);
        getConditionFactory(maxWaitMilliseconds).until(() -> {
            AbstractExecutable job = executableManager.getJob(jobId);
            ExecutableState status = job.getStatus();
            return status == ExecutableState.SUCCEED || status == ExecutableState.ERROR
                    || status == ExecutableState.PAUSED || status == ExecutableState.DISCARDED
                    || status == ExecutableState.SUICIDAL;
        });
    }

    private static ConditionFactory getConditionFactory(long maxWaitMilliseconds) {
        return with().pollInterval(10, TimeUnit.MILLISECONDS) //
                .and().with().pollDelay(10, TimeUnit.MILLISECONDS) //
                .await().atMost(maxWaitMilliseconds, TimeUnit.MILLISECONDS);
    }
}
