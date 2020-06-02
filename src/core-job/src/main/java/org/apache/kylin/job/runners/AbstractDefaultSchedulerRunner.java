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
package org.apache.kylin.job.runners;

import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;

public abstract class AbstractDefaultSchedulerRunner implements Runnable {

    protected final NDefaultScheduler nDefaultScheduler;

    protected final ExecutableContext context;

    protected final String project;


    public AbstractDefaultSchedulerRunner(final NDefaultScheduler nDefaultScheduler) {
        this.nDefaultScheduler = nDefaultScheduler;
        this.context = nDefaultScheduler.getContext();
        this.project = nDefaultScheduler.getProject();
    }

    private boolean checkEpochIdFailed() {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv() && !EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).checkEpochId(context.getEpochId(), project)) {
            nDefaultScheduler.forceShutdown();
            return true;
        }
        return false;
    }

    @Override
    public void run() {
        if (checkEpochIdFailed()) {
            return;
        }

        doRun();
    }

    protected abstract void doRun();
}
