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

import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

import static org.apache.kylin.common.exception.CommonErrorCode.LICENSE_OVER_CAPACITY;

public class LicenseCapacityCheckRunner extends AbstractDefaultSchedulerRunner {
    private static final Logger logger = LoggerFactory.getLogger(LicenseCapacityCheckRunner.class);

    public LicenseCapacityCheckRunner(NDefaultScheduler nDefaultScheduler) {
        super(nDefaultScheduler);
    }

    @Override
    protected void doRun() {
        logger.info("start check license capacity for project {}", project);
        context.setLicenseOverCapacity(isLicenseOverCapacity());
    }

    private boolean isLicenseOverCapacity() {
        val sourceUsageManager = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv());

        try {
            sourceUsageManager.checkIsOverCapacity(project);
        } catch (KylinException e) {
            if (LICENSE_OVER_CAPACITY.toErrorCode() == e.getErrorCode()) {
                logger.warn("Source usage over capacity, no job will be scheduled.", e);
                return true;
            }
        } catch (Throwable e) {
            logger.warn("Check source usage over capacity failed.", e);
        }

        // not over capacity
        return false;
    }
}
