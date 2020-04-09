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

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.epoch.EpochNotMatchException;
import lombok.val;
import org.apache.kylin.common.KylinConfig;

public class EnhancedUnitOfWork implements IKeep {

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName) {
        return doInTransactionWithCheckAndRetry(f, unitName, 3);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes) {
        return doInTransactionWithCheckAndRetry(UnitOfWorkParams.<T>builder().processor(f).unitName(unitName).maxRetry(retryTimes).build());
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWorkParams<T> params) {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.isUTEnv()) {
            params.setEpochChecker(() -> {
                if (!EpochManager.getInstance(config).checkEpochOwner(params.getUnitName())) {
                    throw new EpochNotMatchException("System is trying to recover, please try again later", params.getUnitName());
                }
                return null;
            });
        }
        return UnitOfWork.doInTransactionWithRetry(params);
    }
}
