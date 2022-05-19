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
package io.kyligence.kap.rest.delegate;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.persistence.metadata.JdbcMetadataStore;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.rest.service.JobMetadataService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class JobMetadataInvoker {

    private static JobMetadataContract delegate = null;

    public static void setDelegate(JobMetadataContract delegate) {
        if (JobMetadataInvoker.delegate != null) {
            log.warn("Delegate is replaced as {}, origin value is {}", delegate, JobMetadataInvoker.delegate);
        }
        JobMetadataInvoker.delegate = delegate;
    }

    private JobMetadataContract getDelegate() {
        if (JobMetadataInvoker.delegate == null) {
            // Generally delegate will be set in ContractConfig, here is used for test
            if (SpringContext.getApplicationContext() != null) {
                return SpringContext.getBean(JobMetadataContract.class);
            } else {
                return new JobMetadataService();
            }
        }
        return JobMetadataInvoker.delegate;
    }

    public static JobMetadataInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (!(metadataStore instanceof JdbcMetadataStore) && KylinConfig.getInstanceFromEnv().isDataLoadingNode()) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null) {
            // for UT
            return new JobMetadataInvoker();
        } else {
            return SpringContext.getBean(JobMetadataInvoker.class);
        }
    }

    public void updateStatistics(String project, long date, String model, long duration, long byteSize, int deltaCount) {
        getDelegate().updateStatistics(project, date, model, duration, byteSize, deltaCount);
    }

}