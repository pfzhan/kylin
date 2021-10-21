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
package io.kyligence.kap.metadata.project;

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.EPOCH_DOES_NOT_BELONG_TO_CURRENT_NODE;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;

import io.kyligence.kap.common.persistence.metadata.JdbcMetadataStore;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.epoch.EpochNotMatchException;
import lombok.val;

public class EnhancedUnitOfWork {

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName) {
        return doInTransactionWithCheckAndRetry(f, UnitOfWork.DEFAULT_EPOCH_ID, unitName);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, long epochId, String unitName) {
        return doInTransactionWithCheckAndRetry(f, unitName, UnitOfWork.DEFAULT_MAX_RETRY, epochId);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes) {
        return doInTransactionWithCheckAndRetry(f, unitName, retryTimes, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes,
            long epochId) {
        return doInTransactionWithCheckAndRetry(f, unitName, retryTimes, epochId, null);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes,
            long epochId, String tempLockName) {
        return doInTransactionWithCheckAndRetry(UnitOfWorkParams.<T> builder().processor(f).unitName(unitName)
                .epochId(epochId).maxRetry(retryTimes).tempLockName(tempLockName).build());
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWorkParams<T> params) {
        val config = KylinConfig.getInstanceFromEnv();
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(config).getMetadataStore();
        if (!config.isUTEnv() && metadataStore instanceof JdbcMetadataStore) {
            params.setEpochChecker(() -> {
                if (!EpochManager.getInstance().checkEpochOwner(params.getUnitName())) {
                    throw new EpochNotMatchException(EPOCH_DOES_NOT_BELONG_TO_CURRENT_NODE, params.getUnitName());
                }
                return null;
            });
        }
        return UnitOfWork.doInTransactionWithRetry(params);
    }
}
