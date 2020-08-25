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
package io.kyligence.kap.rest.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AsyncTaskService {

    private static final String GLOBAL = "global";

    @Async
    public void cleanupStorage() throws Exception {

        long startAt = System.currentTimeMillis();
        try {
            val storageCleaner = new StorageCleaner();
            storageCleaner.execute();
        } catch (Exception e) {
            NMetricsGroup.hostTagCounterInc(NMetricsName.STORAGE_CLEAN_FAILED, NMetricsCategory.GLOBAL, GLOBAL);
            throw e;
        } finally {
            NMetricsGroup.hostTagCounterInc(NMetricsName.STORAGE_CLEAN, NMetricsCategory.GLOBAL, GLOBAL);
            NMetricsGroup.hostTagCounterInc(NMetricsName.STORAGE_CLEAN_DURATION, NMetricsCategory.GLOBAL, GLOBAL,
                    System.currentTimeMillis() - startAt);
        }
    }
}
